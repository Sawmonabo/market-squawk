//! Lease-gated construction of production live and research adapters.

mod account;
mod alpaca;
pub(crate) mod credentials;
mod direct;
mod kraken_l3;
mod market_config;
pub(crate) mod nasdaq_reference;
pub(crate) mod openfigi_identity;
mod specs;
mod tradier;

use std::{
    fmt,
    num::{NonZeroU16, NonZeroU32, NonZeroU64},
    sync::Arc,
};

use bytes::Bytes;
use market_squawk_adapter_bls::{BlsAuthorization, BlsRegistrationKey, BlsSource, BlsSourceConfig};
use market_squawk_adapter_files::FileExtractionSource;
use market_squawk_adapter_fred::{FredApiKey, FredOperation, FredRightsPolicy, FredSource};
use market_squawk_adapter_portfolio::PortfolioManifestExtractionSource;
use market_squawk_adapter_sec::{SecContact, SecEdgarSource};
use market_squawk_adapter_treasury::{TreasurySource, TreasurySourceConfig};
use market_squawk_data::{RightsBasis, SourceOperation};
use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::AppConfig;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::application::{
    ManagedResearchExtractionSource, ProductionResearchIngestCoordinator,
    ResearchProviderRuntimeGeneration, ResearchProviderRuntimeMutationAuthority,
    ResearchProviderRuntimeReplacement, ResearchRightsAuthority,
};
use crate::provider_onboarding::ProviderOnboardingMutationAuthority;
use crate::{
    ProductionLiveSourceComposition, ProductionSourceProvider, ProviderActivationLease,
    ProviderOnboardingError, ProviderOnboardingService,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationMode, DataUseOperation, DiscoveryRequest,
    ExtractionRequest, ExtractionSource, FRED_ALFRED_API_SURFACE_ID, ProviderRateAuthority,
    ProviderRateDeclaration, SourceMetadata, SourceMetadataProvider,
};
use specs::BlsAdapterConfiguration;

pub use account::{ProviderAccountActivationError, ProviderAccountBinding, ProviderMarketAccount};
pub use alpaca::{AlpacaBasicAccountActivation, AlpacaBasicActivationError};
pub use direct::{CoinbaseDirectAccountActivation, CoinbaseDirectRuntimeAdmission};
pub use kraken_l3::{
    KrakenL3AccountActivation, KrakenL3ActivationError, KrakenL3WebSocketTokenMaterial,
};
pub use market_config::{
    AlpacaBasicMarketConfigurationInput, BoundedMarketDataInstrumentSet,
    BoundedMarketInstrumentSet, KrakenL3MarketConfigurationInput, MarketConfigAuthorityRequirement,
    MarketDataInstrumentBinding, MarketDataSubscriptionSymbolEvidence, MarketInstrumentBinding,
    MarketInstrumentReferenceBinding, MarketProviderConfigurationError, MarketSourceEvidence,
    MarketSubscriptionPriority, PreparedAlpacaBasicMarketConfiguration,
    PreparedKrakenL3MarketConfiguration, PreparedMarketProviderConfiguration,
    PreparedTradierMarketConfiguration, ProviderMarketConfigurationRequest,
    TradierMarketConfigurationInput,
};
pub use specs::{
    BlsAdapterActivation, COINBASE_DIRECT_MAXIMUM_SUBSCRIPTIONS, CoinbaseDirectActivationSpecError,
    CoinbaseDirectAdapterActivation, CoinbaseDirectProductActivation,
    ControlledLocalFileAdapterActivation, FredAdapterActivation, LocalFileAdapterActivation,
    PortfolioAdapterActivation, ProviderAdapterActivationError, ProviderAdapterActivationRequest,
    SecAdapterActivation, TreasuryAdapterActivation,
};
pub use tradier::{
    TradierMarketDataAccountActivation, TradierMarketDataActivationError,
    TradierStreamingActivation,
};

const COINBASE_SURFACE: &str = "coinbase.public-market-data";
const KRAKEN_SURFACE: &str = "kraken.spot-public-market-data";
const SEC_SURFACE: &str = "sec.edgar-public";
const BLS_PUBLIC_SURFACE: &str = "bls.v1-unregistered";
const BLS_REGISTERED_SURFACE: &str = "bls.v2-registered";
const TREASURY_XML_SURFACE: &str = "treasury.daily-rates-xml";
const TREASURY_FISCAL_SURFACE: &str = "treasury.fiscal-data";
const FRED_SURFACE: &str = FRED_ALFRED_API_SURFACE_ID;
const LOCAL_FILES_SURFACE: &str = "local.files";
const PORTFOLIO_SURFACE: &str = "local.portfolio-imports";
const MAXIMUM_EPHEMERAL_DISCOVERY_PAGES: u16 = 64;

/// One verified provider page returned without durable research publication.
#[derive(Clone, Debug)]
pub(crate) struct FredEphemeralInspectionPage {
    object_id: SourceIdentifier,
    page_evidence: ExactPayloadEvidence,
    received_at: Timestamp,
    canonical_payloads: Vec<Bytes>,
}

impl FredEphemeralInspectionPage {
    pub(crate) const fn object_id(&self) -> &SourceIdentifier {
        &self.object_id
    }

    pub(crate) const fn page_evidence(&self) -> &ExactPayloadEvidence {
        &self.page_evidence
    }

    pub(crate) const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    pub(crate) fn canonical_payloads(&self) -> &[Bytes] {
        &self.canonical_payloads
    }
}

/// Application-owned activation authority shared by CLI, MCP, and local onboarding transports.
pub struct ProviderAdapterActivation {
    onboarding: Arc<ProviderOnboardingService>,
    research: Arc<ProductionResearchIngestCoordinator>,
    research_mutation: ResearchProviderRuntimeMutationAuthority,
    app_config: AppConfig,
    provider_rate: ProviderRateAuthority,
}

impl ProviderAdapterActivation {
    /// Binds the sole onboarding authority, research coordinator, and validated live configuration.
    #[must_use]
    pub(crate) fn new(
        onboarding: Arc<ProviderOnboardingService>,
        research: Arc<ProductionResearchIngestCoordinator>,
        research_mutation: ResearchProviderRuntimeMutationAuthority,
        app_config: AppConfig,
        provider_rate: ProviderRateAuthority,
    ) -> Self {
        Self {
            onboarding,
            research,
            research_mutation,
            app_config,
            provider_rate,
        }
    }

    /// Retrieves and revalidates one bounded FRED page without publishing durable source state.
    ///
    /// The request still uses the product-wide provider-rate authority, an exact active
    /// onboarding lease, current scoped rights, and the platform-managed credential generation.
    /// Provider bytes remain process-local and are discarded after the typed result is built.
    ///
    /// # Errors
    ///
    /// Fails closed for a mismatched lease, invalid credential or rights policy, unavailable
    /// registry authority, incomplete discovery, provider protocol failure, or cancellation.
    #[allow(
        clippy::too_many_arguments,
        reason = "all inspection authority and resource bounds remain explicit"
    )]
    pub(crate) async fn inspect_fred_ephemeral(
        &self,
        lease: ProviderActivationLease,
        spec: FredAdapterActivation,
        dataset: SourceIdentifier,
        page_index: u16,
        page_records: NonZeroU16,
        max_bytes: NonZeroU64,
        deadline: Timestamp,
        cancellation: CancellationToken,
    ) -> Result<FredEphemeralInspectionPage, ProviderAdapterActivationError> {
        require_surface(&lease, FRED_SURFACE)?;
        if page_index >= MAXIMUM_EPHEMERAL_DISCOVERY_PAGES {
            return Err(ProviderAdapterActivationError::SourceBinding);
        }
        if cancellation.is_cancelled() {
            return Err(ProviderAdapterActivationError::Cancelled);
        }
        self.bind_authorization_subject(&spec.metadata)?;
        let secret = self
            .onboarding
            .read_secret_for_activation_request(&lease, cancellation.clone())
            .await?;
        let key = FredApiKey::try_new(secret.expose_secret().to_owned())?;
        let source = FredSource::try_new_for_ephemeral_inspection(
            spec.metadata,
            key,
            spec.policy,
            page_records,
        )?;
        let mut registry = AuthoritativeSourceRegistry::try_new_in_memory_for_bounded_extraction(
            Arc::new(self.provider_rate.clone()),
            self.provider_rate.clone(),
        )?;
        let operation = async {
            let registered = registry.register(source.metadata().clone(), lease.issued_at())?;
            let authority = registry.extraction_authority(&registered, &source)?;
            let max_pages = NonZeroU16::new(MAXIMUM_EPHEMERAL_DISCOVERY_PAGES)
                .ok_or(ProviderAdapterActivationError::SourceBinding)?;
            let discovery_request = DiscoveryRequest::try_new(dataset, None, max_pages, deadline)?;
            let discovered = source
                .discover(authority.clone(), discovery_request, cancellation.clone())
                .await?;
            let object = discovered
                .objects()
                .get(usize::from(page_index))
                .cloned()
                .ok_or(ProviderAdapterActivationError::SourceBinding)?;
            let max_records = NonZeroU32::new(u32::from(page_records.get()))
                .ok_or(ProviderAdapterActivationError::SourceBinding)?;
            let extraction_request =
                ExtractionRequest::try_new(object.clone(), max_records, max_bytes, deadline)?;
            let page = source
                .extract_page_ephemeral(&authority, &extraction_request, cancellation)
                .await?;
            Ok::<_, ProviderAdapterActivationError>(FredEphemeralInspectionPage {
                object_id: object.object_id().clone(),
                page_evidence: page.page_evidence().clone(),
                received_at: page.received_at(),
                canonical_payloads: page.canonical_payloads().to_vec(),
            })
        }
        .await;
        let shutdown = registry
            .shutdown()
            .map_err(ProviderAdapterActivationError::from);
        match operation {
            Ok(page) => {
                shutdown?;
                Ok(page)
            }
            Err(error) => {
                shutdown?;
                Err(error)
            }
        }
    }

    /// Completes any ready credential verification and activates the matching concrete adapter.
    ///
    /// Durable provider verification is resumed without another provider check. A duplicate
    /// runtime profile is rejected because adapter-specific request state is not interchangeable.
    ///
    /// # Errors
    ///
    /// Fails closed for refresh-required or rights-blocked profiles, request/surface mismatch,
    /// cancellation, invalid provider configuration, or durable registry rejection.
    pub(crate) async fn activate_ready_profile(
        &self,
        session_id: Uuid,
        request: ProviderAdapterActivationRequest,
        cancellation: CancellationToken,
    ) -> Result<ProviderActivationOutcome, ProviderAdapterActivationError> {
        if cancellation.is_cancelled() {
            return Err(ProviderAdapterActivationError::Cancelled);
        }
        let lease = match self.onboarding.activation_lease(session_id) {
            Ok(lease) => lease,
            Err(ProviderOnboardingError::ActivationUnavailable) => {
                self.onboarding
                    .activate(session_id, cancellation.clone())
                    .await?
            }
            Err(error) => return Err(error.into()),
        };
        if cancellation.is_cancelled() {
            return Err(ProviderAdapterActivationError::Cancelled);
        }
        self.activate_with_lease(lease, request, cancellation).await
    }

    /// Activates a research adapter only when current onboarding authority still matches the
    /// exact generation already selected for durable publication.
    pub(crate) async fn activate_exact_research_profile(
        &self,
        expected: &ResearchProviderRuntimeGeneration,
        request: ProviderAdapterActivationRequest,
        cancellation: CancellationToken,
    ) -> Result<ProviderActivationOutcome, ProviderAdapterActivationError> {
        if cancellation.is_cancelled() {
            return Err(ProviderAdapterActivationError::Cancelled);
        }
        let lease = self.onboarding.activation_lease(expected.session_id())?;
        let candidate = self.runtime_generation_for_request(&lease, &request)?;
        if &candidate != expected {
            return Err(ProviderAdapterActivationError::SourceBinding);
        }
        self.activate_with_lease(lease, request, cancellation).await
    }

    /// Reconstructs an adapter only from an already-active durable onboarding lease.
    ///
    /// Unlike [`Self::activate_ready_profile`], restart recovery never performs provider
    /// verification or changes onboarding state. Expired, refresh-required, or otherwise inactive
    /// sessions fail closed.
    ///
    /// # Errors
    ///
    /// Fails closed when the durable session is not active or when the retained adapter request
    /// no longer satisfies its exact provider contract.
    pub(crate) fn restore_active_profile(
        &self,
        session_id: Uuid,
        request: ProviderAdapterActivationRequest,
    ) -> Result<ProviderActivationOutcome, ProviderAdapterActivationError> {
        let lease = self.onboarding.activation_lease(session_id)?;
        self.restore_with_lease(lease, request)
    }

    /// Returns the exact provider generation currently published into the research runtime.
    pub(crate) fn research_runtime_generation(
        &self,
        profile: &SourceIdentifier,
    ) -> Result<Option<ResearchProviderRuntimeGeneration>, ProviderAdapterActivationError> {
        self.research
            .provider_runtime_generation(profile)
            .map_err(Into::into)
    }

    /// Returns one coherent nonblocking count of callable research provider runtimes.
    pub(crate) fn active_research_runtime_count(
        &self,
    ) -> Result<usize, ProviderAdapterActivationError> {
        self.research
            .active_provider_runtime_count()
            .map_err(Into::into)
    }

    /// Returns the exact fixed discovery dataset carried by one callable research adapter.
    pub(crate) fn registered_discovery_dataset(
        &self,
        profile: &SourceIdentifier,
    ) -> Result<Option<SourceIdentifier>, ProviderAdapterActivationError> {
        self.research
            .registered_discovery_dataset(profile)
            .map_err(Into::into)
    }

    /// Revokes the exact research generation and all in-flight or retained request authority.
    pub(crate) async fn revoke_research_runtime(
        &self,
        expected: &ResearchProviderRuntimeGeneration,
    ) -> Result<(), ProviderAdapterActivationError> {
        let _onboarding_authority = self.onboarding.acquire_runtime_mutation_authority().await;
        self.research_mutation
            .revoke_provider_generation(expected.profile(), expected)
            .await
            .map_err(Into::into)
    }

    /// Returns whether one exact runtime still has the current onboarding activation lease.
    pub(crate) async fn research_runtime_lease_is_current(
        &self,
        expected: &ResearchProviderRuntimeGeneration,
    ) -> Result<bool, ProviderAdapterActivationError> {
        let onboarding_authority = self.onboarding.acquire_runtime_mutation_authority().await;
        runtime_lease_is_current(&onboarding_authority, expected)
    }

    /// Rolls back a prepared candidate only while the predecessor lease remains exactly current.
    pub(crate) async fn rollback_prepared_research_replacement(
        &self,
        mut prepared: PreparedProviderAdapterReplacement,
    ) -> Result<ResearchProviderRuntimeGeneration, ProviderAdapterActivationError> {
        let onboarding_authority = self.onboarding.acquire_runtime_mutation_authority().await;
        if runtime_lease_is_current(&onboarding_authority, prepared.candidate())? {
            return Err(ProviderAdapterActivationError::SourceBinding);
        }
        let lease = onboarding_authority.active_lease(prepared.expected().session_id())?;
        require_runtime_lease(prepared.expected(), &lease)?;
        let transaction = prepared
            .transaction
            .take()
            .ok_or(ProviderAdapterActivationError::SourceBinding)?;
        self.research_mutation
            .rollback(transaction)
            .map_err(Into::into)
    }

    /// Rolls back a committed candidate only while the predecessor lease remains exactly current.
    pub(crate) async fn rollback_committed_research_replacement(
        &self,
        mut committed: CommittedProviderAdapterReplacement,
    ) -> Result<ResearchProviderRuntimeGeneration, ProviderAdapterActivationError> {
        let onboarding_authority = self.onboarding.acquire_runtime_mutation_authority().await;
        if runtime_lease_is_current(&onboarding_authority, committed.candidate())? {
            return Err(ProviderAdapterActivationError::SourceBinding);
        }
        let lease = onboarding_authority.active_lease(committed.expected().session_id())?;
        require_runtime_lease(committed.expected(), &lease)?;
        let transaction = committed
            .transaction
            .take()
            .ok_or(ProviderAdapterActivationError::SourceBinding)?;
        self.research_mutation
            .rollback(transaction)
            .map_err(Into::into)
    }

    pub(crate) async fn discard_prepared_replacement_candidate(
        &self,
        prepared: &PreparedProviderAdapterReplacement,
        evidence_digest: EvidenceDigest,
    ) -> Result<(), ProviderAdapterActivationError> {
        self.discard_replacement_candidate(
            &prepared.lease,
            prepared.expected(),
            prepared.candidate(),
            evidence_digest,
        )
        .await
    }

    pub(crate) async fn discard_committed_replacement_candidate(
        &self,
        committed: &CommittedProviderAdapterReplacement,
        evidence_digest: EvidenceDigest,
    ) -> Result<(), ProviderAdapterActivationError> {
        self.discard_replacement_candidate(
            &committed.lease,
            committed.expected(),
            committed.candidate(),
            evidence_digest,
        )
        .await
    }

    async fn discard_replacement_candidate(
        &self,
        candidate_lease: &ProviderActivationLease,
        expected: &ResearchProviderRuntimeGeneration,
        candidate: &ResearchProviderRuntimeGeneration,
        evidence_digest: EvidenceDigest,
    ) -> Result<(), ProviderAdapterActivationError> {
        let onboarding_authority = self.onboarding.acquire_runtime_mutation_authority().await;
        let predecessor = onboarding_authority.active_lease(expected.session_id())?;
        require_runtime_lease(expected, &predecessor)?;
        if runtime_lease_is_current(&onboarding_authority, candidate)? {
            onboarding_authority
                .invalidate_activation_recipe(candidate.session_id(), evidence_digest)?;
        } else if candidate.credential_generation().is_some() {
            onboarding_authority
                .rollback_prepared_activation(
                    candidate_lease,
                    evidence_digest,
                    CancellationToken::new(),
                )
                .await?;
        } else {
            onboarding_authority
                .invalidate_activation_recipe(candidate.session_id(), evidence_digest)?;
        }
        let predecessor = onboarding_authority.active_lease(expected.session_id())?;
        require_runtime_lease(expected, &predecessor)
    }

    /// Revokes and drains the exact predecessor while onboarding currentness cannot change.
    pub(crate) async fn revoke_replacement_predecessor(
        &self,
        prepared: &mut PreparedProviderAdapterReplacement,
    ) -> Result<(), ProviderAdapterActivationError> {
        let onboarding_authority = self.onboarding.acquire_runtime_mutation_authority().await;
        onboarding_authority.require_prepared_or_active(&prepared.lease)?;
        let predecessor = onboarding_authority.active_lease(prepared.expected().session_id())?;
        require_runtime_lease(prepared.expected(), &predecessor)?;
        let transaction = prepared
            .transaction
            .as_mut()
            .ok_or(ProviderAdapterActivationError::SourceBinding)?;
        self.research_mutation
            .revoke_predecessor(transaction)
            .await
            .map_err(Into::into)
    }

    /// Transfers one drained candidate into the sealed pending-runtime state.
    pub(crate) async fn commit_research_replacement(
        &self,
        prepared: &mut PreparedProviderAdapterReplacement,
    ) -> Result<CommittedProviderAdapterReplacement, ProviderAdapterActivationError> {
        let onboarding_authority = self.onboarding.acquire_runtime_mutation_authority().await;
        onboarding_authority.require_prepared_or_active(&prepared.lease)?;
        let predecessor = onboarding_authority.active_lease(prepared.expected().session_id())?;
        require_runtime_lease(prepared.expected(), &predecessor)?;
        let transaction = prepared
            .transaction
            .as_mut()
            .ok_or(ProviderAdapterActivationError::SourceBinding)?;
        self.research_mutation.commit(transaction)?;
        Ok(CommittedProviderAdapterReplacement {
            lease: prepared.lease.clone(),
            expected: prepared.expected.clone(),
            candidate: prepared.candidate.clone(),
            transaction: prepared.transaction.take(),
        })
    }

    /// Commits the exact candidate onboarding generation only for this sealed replacement.
    pub(crate) async fn commit_replacement_onboarding(
        &self,
        committed: &CommittedProviderAdapterReplacement,
    ) -> Result<ProviderActivationLease, ProviderAdapterActivationError> {
        let onboarding_authority = self.onboarding.acquire_runtime_mutation_authority().await;
        onboarding_authority.require_prepared_or_active(&committed.lease)?;
        let predecessor = onboarding_authority.active_lease(committed.expected().session_id())?;
        require_runtime_lease(committed.expected(), &predecessor)?;
        let active = onboarding_authority.commit_prepared_activation(&committed.lease)?;
        require_runtime_lease(committed.candidate(), &active)?;
        Ok(active)
    }

    /// Retires an exact still-current predecessor only after durable cutover authority exists.
    pub(crate) async fn retire_replacement_predecessor(
        &self,
        committed: &CommittedProviderAdapterReplacement,
        cutover_digest: EvidenceDigest,
    ) -> Result<(), ProviderAdapterActivationError> {
        let onboarding_authority = self.onboarding.acquire_runtime_mutation_authority().await;
        let candidate = onboarding_authority.active_lease(committed.candidate().session_id())?;
        require_runtime_lease(committed.candidate(), &candidate)?;
        if runtime_lease_is_current(&onboarding_authority, committed.expected())? {
            onboarding_authority
                .invalidate_activation_recipe(committed.expected().session_id(), cutover_digest)?;
        }
        if runtime_lease_is_current(&onboarding_authority, committed.expected())? {
            return Err(ProviderAdapterActivationError::SourceBinding);
        }
        Ok(())
    }

    /// Publishes a committed candidate only while its exact onboarding lease remains current.
    pub(crate) async fn finalize_research_replacement(
        &self,
        committed: &mut CommittedProviderAdapterReplacement,
    ) -> Result<ActivatedResearchProvider, ProviderAdapterActivationError> {
        let onboarding_authority = self.onboarding.acquire_runtime_mutation_authority().await;
        if runtime_lease_is_current(&onboarding_authority, committed.expected())? {
            return Err(ProviderAdapterActivationError::SourceBinding);
        }
        let lease = onboarding_authority.active_lease(committed.candidate().session_id())?;
        require_runtime_lease(committed.candidate(), &lease)?;
        let transaction = committed
            .transaction
            .as_mut()
            .ok_or(ProviderAdapterActivationError::SourceBinding)?;
        let profile = lease.surface_id().clone();
        let generation = self.research_mutation.finalize(transaction)?;
        if generation != committed.candidate {
            return Err(ProviderAdapterActivationError::SourceBinding);
        }
        committed.transaction = None;
        Ok(ActivatedResearchProvider {
            lease,
            profile,
            generation,
        })
    }

    /// Derives the exact non-secret runtime identity before adapter publication.
    pub(crate) fn runtime_generation_for_request(
        &self,
        lease: &ProviderActivationLease,
        request: &ProviderAdapterActivationRequest,
    ) -> Result<ResearchProviderRuntimeGeneration, ProviderAdapterActivationError> {
        let (metadata, rights) = match request {
            ProviderAdapterActivationRequest::Sec(spec) => (
                &spec.metadata,
                provider_research_rights(lease, spec.metadata.source_id())?,
            ),
            ProviderAdapterActivationRequest::Bls(spec) => (
                &spec.metadata,
                provider_research_rights(lease, spec.metadata.source_id())?,
            ),
            ProviderAdapterActivationRequest::Treasury(spec) => (
                &spec.metadata,
                provider_research_rights(lease, spec.metadata.source_id())?,
            ),
            ProviderAdapterActivationRequest::Fred(spec) => (
                &spec.metadata,
                fred_research_rights(lease, spec.metadata.source_id(), &spec.policy)?,
            ),
            ProviderAdapterActivationRequest::ControlledLocalFiles(spec) => (
                &spec.metadata,
                controlled_local_file_rights(lease, spec.metadata.source_id(), &spec.evidence)?,
            ),
            ProviderAdapterActivationRequest::Live(_)
            | ProviderAdapterActivationRequest::CoinbaseDirect(_)
            | ProviderAdapterActivationRequest::LocalFiles(_)
            | ProviderAdapterActivationRequest::Portfolio(_) => {
                return Err(ProviderAdapterActivationError::SourceBinding);
            }
        };
        runtime_generation(lease, metadata.clone(), rights)
    }

    /// Fully constructs and reserves an exact credential-generation replacement.
    pub(crate) async fn prepare_research_replacement(
        &self,
        lease: ProviderActivationLease,
        request: ProviderAdapterActivationRequest,
        expected: ResearchProviderRuntimeGeneration,
        cancellation: CancellationToken,
    ) -> Result<PreparedProviderAdapterReplacement, ProviderAdapterActivationError> {
        if cancellation.is_cancelled() {
            return Err(ProviderAdapterActivationError::Cancelled);
        }
        let candidate = self.runtime_generation_for_request(&lease, &request)?;
        self.bind_authorization_subject(candidate.metadata())?;
        let prepared = match request {
            ProviderAdapterActivationRequest::Bls(spec) => {
                require_surface(&lease, BLS_REGISTERED_SURFACE)?;
                let BlsAdapterActivation {
                    metadata,
                    configuration,
                } = spec;
                let BlsAdapterConfiguration::Registered {
                    series,
                    start_year,
                    end_year,
                } = configuration
                else {
                    return Err(ProviderAdapterActivationError::SurfaceMismatch);
                };
                let secret = self
                    .onboarding
                    .read_secret_for_activation_request(&lease, cancellation.clone())
                    .await?;
                let authorization = BlsAuthorization::RegisteredV2(BlsRegistrationKey::try_new(
                    secret.expose_secret().to_owned(),
                )?);
                let rights = provider_research_rights(&lease, metadata.source_id())?;
                let config = BlsSourceConfig::try_new(authorization, series, start_year, end_year)?;
                let source = BlsSource::try_new(metadata, config)?;
                self.prepare_runtime_replacement(
                    &lease,
                    expected,
                    candidate.clone(),
                    source,
                    rights,
                )
                .await?
            }
            ProviderAdapterActivationRequest::Treasury(spec) => {
                let matches = matches!(
                    (lease.surface_id().as_str(), &spec.config),
                    (
                        TREASURY_FISCAL_SURFACE,
                        TreasurySourceConfig::AverageInterestRates(_)
                    ) | (TREASURY_XML_SURFACE, TreasurySourceConfig::DailyRates(_))
                );
                if !matches {
                    return Err(ProviderAdapterActivationError::SurfaceMismatch);
                }
                let rights = provider_research_rights(&lease, spec.metadata.source_id())?;
                let source = TreasurySource::try_new(spec.metadata, spec.config)?;
                self.prepare_runtime_replacement(
                    &lease,
                    expected,
                    candidate.clone(),
                    source,
                    rights,
                )
                .await?
            }
            ProviderAdapterActivationRequest::Fred(spec) => {
                require_surface(&lease, FRED_SURFACE)?;
                let secret = self
                    .onboarding
                    .read_secret_for_activation_request(&lease, cancellation.clone())
                    .await?;
                let key = FredApiKey::try_new(secret.expose_secret().to_owned())?;
                let rights = fred_research_rights(&lease, spec.metadata.source_id(), &spec.policy)?;
                let source = FredSource::try_new(spec.metadata, key, spec.policy)?;
                self.prepare_runtime_replacement(
                    &lease,
                    expected,
                    candidate.clone(),
                    source,
                    rights,
                )
                .await?
            }
            ProviderAdapterActivationRequest::ControlledLocalFiles(spec) => {
                require_surface(&lease, LOCAL_FILES_SURFACE)?;
                let rights = controlled_local_file_rights(
                    &lease,
                    spec.metadata.source_id(),
                    &spec.evidence,
                )?;
                let source = FileExtractionSource::try_new_controlled_import(
                    spec.metadata,
                    spec.root,
                    spec.representation_state_root,
                    spec.manifest,
                    spec.limits,
                )?;
                self.prepare_runtime_replacement(
                    &lease,
                    expected,
                    candidate.clone(),
                    source,
                    rights,
                )
                .await?
            }
            ProviderAdapterActivationRequest::Live(_)
            | ProviderAdapterActivationRequest::CoinbaseDirect(_)
            | ProviderAdapterActivationRequest::Sec(_)
            | ProviderAdapterActivationRequest::LocalFiles(_)
            | ProviderAdapterActivationRequest::Portfolio(_) => {
                return Err(ProviderAdapterActivationError::SourceBinding);
            }
        };
        if cancellation.is_cancelled() {
            return Err(ProviderAdapterActivationError::Cancelled);
        }
        let expected = prepared.expected().clone();
        Ok(PreparedProviderAdapterReplacement {
            lease,
            expected,
            candidate,
            transaction: Some(prepared),
        })
    }

    async fn prepare_runtime_replacement<S>(
        &self,
        candidate_lease: &ProviderActivationLease,
        expected: ResearchProviderRuntimeGeneration,
        candidate: ResearchProviderRuntimeGeneration,
        source: S,
        rights: ResearchRightsAuthority,
    ) -> Result<ResearchProviderRuntimeReplacement, ProviderAdapterActivationError>
    where
        S: ManagedResearchExtractionSource,
    {
        let onboarding_authority = self.onboarding.acquire_runtime_mutation_authority().await;
        onboarding_authority.require_prepared_or_active(candidate_lease)?;
        let predecessor = onboarding_authority.active_lease(expected.session_id())?;
        require_runtime_lease(&expected, &predecessor)?;
        self.research_mutation
            .prepare_provider_replacement(expected, candidate, source, rights)
            .map_err(Into::into)
    }

    async fn activate_with_lease(
        &self,
        lease: ProviderActivationLease,
        request: ProviderAdapterActivationRequest,
        cancellation: CancellationToken,
    ) -> Result<ProviderActivationOutcome, ProviderAdapterActivationError> {
        if cancellation.is_cancelled() {
            return Err(ProviderAdapterActivationError::Cancelled);
        }
        match request {
            ProviderAdapterActivationRequest::Live(routes) => {
                self.activate_live(lease, routes).map(Into::into)
            }
            ProviderAdapterActivationRequest::CoinbaseDirect(spec) => {
                direct::activate_coinbase_direct(
                    Arc::clone(&self.onboarding),
                    self.app_config.clone(),
                    self.provider_rate.clone(),
                    lease,
                    spec,
                )
                .map(Into::into)
            }
            ProviderAdapterActivationRequest::Sec(spec) => {
                self.activate_sec(lease, spec).map(Into::into)
            }
            ProviderAdapterActivationRequest::Bls(spec) => self
                .activate_bls(lease, spec, cancellation)
                .await
                .map(Into::into),
            ProviderAdapterActivationRequest::Treasury(spec) => {
                self.activate_treasury(lease, spec).map(Into::into)
            }
            ProviderAdapterActivationRequest::Fred(spec) => self
                .activate_fred(lease, spec, cancellation)
                .await
                .map(Into::into),
            ProviderAdapterActivationRequest::LocalFiles(spec) => {
                self.activate_local_files(lease, spec).map(Into::into)
            }
            ProviderAdapterActivationRequest::ControlledLocalFiles(spec) => self
                .activate_controlled_local_files(lease, spec)
                .map(Into::into),
            ProviderAdapterActivationRequest::Portfolio(spec) => {
                self.activate_portfolio(lease, spec).map(Into::into)
            }
        }
    }

    fn restore_with_lease(
        &self,
        lease: ProviderActivationLease,
        request: ProviderAdapterActivationRequest,
    ) -> Result<ProviderActivationOutcome, ProviderAdapterActivationError> {
        match request {
            ProviderAdapterActivationRequest::Live(routes) => {
                self.activate_live(lease, routes).map(Into::into)
            }
            ProviderAdapterActivationRequest::CoinbaseDirect(_spec) => {
                require_surface(&lease, "coinbase.exchange-direct-market-data")?;
                Err(ProviderAdapterActivationError::ExplicitResumeRequired)
            }
            ProviderAdapterActivationRequest::Sec(spec) => {
                self.activate_sec(lease, spec).map(Into::into)
            }
            ProviderAdapterActivationRequest::Bls(spec) => {
                self.restore_bls(lease, spec).map(Into::into)
            }
            ProviderAdapterActivationRequest::Treasury(spec) => {
                self.activate_treasury(lease, spec).map(Into::into)
            }
            ProviderAdapterActivationRequest::Fred(_spec) => {
                require_surface(&lease, FRED_SURFACE)?;
                Err(ProviderAdapterActivationError::ExplicitResumeRequired)
            }
            ProviderAdapterActivationRequest::LocalFiles(spec) => {
                self.activate_local_files(lease, spec).map(Into::into)
            }
            ProviderAdapterActivationRequest::ControlledLocalFiles(spec) => self
                .activate_controlled_local_files(lease, spec)
                .map(Into::into),
            ProviderAdapterActivationRequest::Portfolio(spec) => {
                self.activate_portfolio(lease, spec).map(Into::into)
            }
        }
    }

    fn activate_live(
        &self,
        lease: ProviderActivationLease,
        routes: Vec<market_squawk_live::LiveRouteConfig>,
    ) -> Result<LiveProviderActivation, ProviderAdapterActivationError> {
        let provider = match lease.surface_id().as_str() {
            COINBASE_SURFACE => ProductionSourceProvider::Coinbase,
            KRAKEN_SURFACE => ProductionSourceProvider::Kraken,
            _ => return Err(ProviderAdapterActivationError::SurfaceMismatch),
        };
        let composition = ProductionLiveSourceComposition::try_for_provider_with_rate_authority(
            self.app_config.clone(),
            routes,
            provider,
            self.provider_rate.clone(),
        )?;
        Ok(LiveProviderActivation { lease, composition })
    }

    fn activate_sec(
        &self,
        lease: ProviderActivationLease,
        spec: SecAdapterActivation,
    ) -> Result<ActivatedResearchProvider, ProviderAdapterActivationError> {
        require_surface(&lease, SEC_SURFACE)?;
        let organization = lease
            .public_configuration()
            .get("organization")
            .ok_or(ProviderAdapterActivationError::SurfaceMismatch)?;
        let administrative_email = lease
            .public_configuration()
            .get("administrative_email")
            .ok_or(ProviderAdapterActivationError::SurfaceMismatch)?;
        let contact = SecContact::try_new(organization, administrative_email)?;
        let rights = provider_research_rights(&lease, spec.metadata.source_id())?;
        let source = SecEdgarSource::try_new(
            spec.metadata,
            contact,
            market_squawk_sources::install_ring_tls_provider()?,
            spec.raw_store,
            spec.representations,
            spec.identities,
            spec.parser_limits,
        )?;
        self.register(lease, source, rights)
    }

    async fn activate_bls(
        &self,
        lease: ProviderActivationLease,
        spec: BlsAdapterActivation,
        cancellation: CancellationToken,
    ) -> Result<ActivatedResearchProvider, ProviderAdapterActivationError> {
        let BlsAdapterActivation {
            metadata,
            configuration,
        } = spec;
        let config = match (lease.surface_id().as_str(), configuration) {
            (BLS_PUBLIC_SURFACE, BlsAdapterConfiguration::Public(config)) => config,
            (
                BLS_REGISTERED_SURFACE,
                BlsAdapterConfiguration::Registered {
                    series,
                    start_year,
                    end_year,
                },
            ) => {
                let secret = self
                    .onboarding
                    .read_secret_for_activation_request(&lease, cancellation)
                    .await?;
                BlsSourceConfig::try_new(
                    BlsAuthorization::RegisteredV2(BlsRegistrationKey::try_new(
                        secret.expose_secret().to_owned(),
                    )?),
                    series,
                    start_year,
                    end_year,
                )?
            }
            _ => return Err(ProviderAdapterActivationError::SurfaceMismatch),
        };
        let rights = provider_research_rights(&lease, metadata.source_id())?;
        let source = BlsSource::try_new(metadata, config)?;
        self.register(lease, source, rights)
    }

    fn restore_bls(
        &self,
        lease: ProviderActivationLease,
        spec: BlsAdapterActivation,
    ) -> Result<ActivatedResearchProvider, ProviderAdapterActivationError> {
        let BlsAdapterActivation {
            metadata,
            configuration,
        } = spec;
        let config = match (lease.surface_id().as_str(), configuration) {
            (BLS_PUBLIC_SURFACE, BlsAdapterConfiguration::Public(config)) => config,
            (BLS_REGISTERED_SURFACE, BlsAdapterConfiguration::Registered { .. }) => {
                return Err(ProviderAdapterActivationError::ExplicitResumeRequired);
            }
            _ => return Err(ProviderAdapterActivationError::SurfaceMismatch),
        };
        let rights = provider_research_rights(&lease, metadata.source_id())?;
        let source = BlsSource::try_new(metadata, config)?;
        self.register(lease, source, rights)
    }

    fn activate_treasury(
        &self,
        lease: ProviderActivationLease,
        spec: TreasuryAdapterActivation,
    ) -> Result<ActivatedResearchProvider, ProviderAdapterActivationError> {
        let matches = matches!(
            (lease.surface_id().as_str(), &spec.config),
            (
                TREASURY_FISCAL_SURFACE,
                TreasurySourceConfig::AverageInterestRates(_)
            ) | (TREASURY_XML_SURFACE, TreasurySourceConfig::DailyRates(_))
        );
        if !matches {
            return Err(ProviderAdapterActivationError::SurfaceMismatch);
        }
        let rights = provider_research_rights(&lease, spec.metadata.source_id())?;
        let source = TreasurySource::try_new(spec.metadata, spec.config)?;
        self.register(lease, source, rights)
    }

    async fn activate_fred(
        &self,
        lease: ProviderActivationLease,
        spec: FredAdapterActivation,
        cancellation: CancellationToken,
    ) -> Result<ActivatedResearchProvider, ProviderAdapterActivationError> {
        require_surface(&lease, FRED_SURFACE)?;
        let secret = self
            .onboarding
            .read_secret_for_activation_request(&lease, cancellation)
            .await?;
        let key = FredApiKey::try_new(secret.expose_secret().to_owned())?;
        let rights = fred_research_rights(&lease, spec.metadata.source_id(), &spec.policy)?;
        let source = FredSource::try_new(spec.metadata, key, spec.policy)?;
        self.register(lease, source, rights)
    }

    fn activate_local_files(
        &self,
        lease: ProviderActivationLease,
        spec: LocalFileAdapterActivation,
    ) -> Result<ActivatedResearchProvider, ProviderAdapterActivationError> {
        require_surface(&lease, LOCAL_FILES_SURFACE)?;
        let source_id = spec.metadata.source_id().clone();
        let rights = ResearchRightsAuthority::try_new(
            source_id,
            RightsBasis::user_owned_local(spec.ownership),
            lease.capability_digest(),
            None,
        )?;
        let source = FileExtractionSource::try_new(
            spec.metadata,
            spec.root,
            spec.representation_state_root,
            spec.manifest,
            spec.limits,
        )?;
        self.register(lease, source, rights)
    }

    fn activate_controlled_local_files(
        &self,
        lease: ProviderActivationLease,
        spec: ControlledLocalFileAdapterActivation,
    ) -> Result<ActivatedResearchProvider, ProviderAdapterActivationError> {
        require_surface(&lease, LOCAL_FILES_SURFACE)?;
        let source_id = spec.metadata.source_id().clone();
        let rights = controlled_local_file_rights(&lease, &source_id, &spec.evidence)?;
        let source = FileExtractionSource::try_new_controlled_import(
            spec.metadata,
            spec.root,
            spec.representation_state_root,
            spec.manifest,
            spec.limits,
        )?;
        self.register(lease, source, rights)
    }

    fn activate_portfolio(
        &self,
        lease: ProviderActivationLease,
        spec: PortfolioAdapterActivation,
    ) -> Result<ActivatedResearchProvider, ProviderAdapterActivationError> {
        require_surface(&lease, PORTFOLIO_SURFACE)?;
        let source_id = spec.metadata.source_id().clone();
        let rights = ResearchRightsAuthority::try_new(
            source_id,
            RightsBasis::user_owned_local(spec.ownership),
            lease.capability_digest(),
            None,
        )?;
        let source = PortfolioManifestExtractionSource::try_new(
            spec.metadata,
            spec.root,
            spec.manifest_reference,
            spec.manifest,
            spec.archive,
            spec.credential,
            spec.limits,
        )?;
        self.register(lease, source, rights)
    }

    fn register<S>(
        &self,
        lease: ProviderActivationLease,
        source: S,
        rights: ResearchRightsAuthority,
    ) -> Result<ActivatedResearchProvider, ProviderAdapterActivationError>
    where
        S: ManagedResearchExtractionSource,
    {
        let profile = lease.surface_id().clone();
        let generation = runtime_generation(&lease, source.metadata().clone(), rights.clone())?;
        self.bind_authorization_subject(generation.metadata())?;
        let onboarding_authority = self.onboarding.try_acquire_runtime_mutation_authority()?;
        onboarding_authority.require_active(&lease)?;
        self.research_mutation
            .register_provider_source(generation.clone(), source, rights)?;
        Ok(ActivatedResearchProvider {
            lease,
            profile,
            generation,
        })
    }

    fn bind_authorization_subject(
        &self,
        metadata: &SourceMetadata,
    ) -> Result<(), ProviderAdapterActivationError> {
        let authorization = metadata.authorization();
        match authorization.mode() {
            AuthorizationMode::UserAuthorized | AuthorizationMode::Licensed => {
                let policy = metadata
                    .budget_policy()
                    .ok_or(ProviderAdapterActivationError::SourceBinding)?;
                let subject = ProviderRateDeclaration::governed_provider_subject(
                    policy.scope().as_source_identifier(),
                )
                .map_err(|_| ProviderAdapterActivationError::SourceBinding)?;
                self.provider_rate.bind_authorization_subject(
                    authorization.mode(),
                    authorization.evidence().content_digest(),
                    &subject,
                )?;
            }
            AuthorizationMode::PublicInterface | AuthorizationMode::UserOwnedLocal => {}
        }
        Ok(())
    }
}

impl fmt::Debug for ProviderAdapterActivation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderAdapterActivation")
            .field("onboarding", &"[ONBOARDING AUTHORITY]")
            .field("research", &"[RESEARCH AUTHORITY]")
            .field("app_config", &self.app_config)
            .finish()
    }
}

/// Fully validated live-source construction retained separately from runtime quality state.
#[derive(Debug)]
pub struct LiveProviderActivation {
    lease: ProviderActivationLease,
    composition: ProductionLiveSourceComposition,
}

impl LiveProviderActivation {
    /// Returns the immutable provider activation authority.
    pub const fn lease(&self) -> &ProviderActivationLease {
        &self.lease
    }

    /// Returns validated source composition without claiming any runtime data quality.
    pub const fn composition(&self) -> &ProductionLiveSourceComposition {
        &self.composition
    }

    /// Consumes the activation into the source composition used by the live runtime.
    pub fn into_composition(self) -> ProductionLiveSourceComposition {
        self.composition
    }
}

/// Completed research adapter registration with immutable provider authority.
#[derive(Clone, Debug)]
pub struct ActivatedResearchProvider {
    lease: ProviderActivationLease,
    profile: SourceIdentifier,
    generation: ResearchProviderRuntimeGeneration,
}

impl ActivatedResearchProvider {
    /// Returns the immutable provider activation authority.
    pub const fn lease(&self) -> &ProviderActivationLease {
        &self.lease
    }

    /// Returns the exact coordinator profile identity.
    pub const fn profile(&self) -> &SourceIdentifier {
        &self.profile
    }

    /// Returns the exact generation published into the research runtime.
    pub const fn generation(&self) -> &ResearchProviderRuntimeGeneration {
        &self.generation
    }
}

/// Fully constructed credential replacement awaiting one serialized runtime publication.
pub(crate) struct PreparedProviderAdapterReplacement {
    lease: ProviderActivationLease,
    expected: ResearchProviderRuntimeGeneration,
    candidate: ResearchProviderRuntimeGeneration,
    transaction: Option<ResearchProviderRuntimeReplacement>,
}

impl PreparedProviderAdapterReplacement {
    /// Returns the exact predecessor generation held by this transaction.
    pub(crate) const fn expected(&self) -> &ResearchProviderRuntimeGeneration {
        &self.expected
    }

    /// Returns the exact replacement generation bound to durable desired state.
    pub(crate) const fn candidate(&self) -> &ResearchProviderRuntimeGeneration {
        &self.candidate
    }
}

impl fmt::Debug for PreparedProviderAdapterReplacement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedProviderAdapterReplacement")
            .field("surface_id", self.lease.surface_id())
            .field("candidate", &self.candidate)
            .finish_non_exhaustive()
    }
}

/// Exact candidate held non-callable until onboarding and durable state jointly authorize it.
pub(crate) struct CommittedProviderAdapterReplacement {
    lease: ProviderActivationLease,
    expected: ResearchProviderRuntimeGeneration,
    candidate: ResearchProviderRuntimeGeneration,
    transaction: Option<ResearchProviderRuntimeReplacement>,
}

impl CommittedProviderAdapterReplacement {
    pub(crate) const fn expected(&self) -> &ResearchProviderRuntimeGeneration {
        &self.expected
    }

    pub(crate) const fn candidate(&self) -> &ResearchProviderRuntimeGeneration {
        &self.candidate
    }

    pub(crate) const fn runtime_is_finalized(&self) -> bool {
        self.transaction.is_none()
    }
}

impl fmt::Debug for CommittedProviderAdapterReplacement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommittedProviderAdapterReplacement")
            .field("surface_id", self.lease.surface_id())
            .field("expected", &self.expected)
            .field("candidate", &self.candidate)
            .finish_non_exhaustive()
    }
}

/// Closed result of a ready-profile activation request.
#[derive(Debug)]
pub enum ProviderActivationOutcome {
    /// Validated live connector construction.
    Live(Box<LiveProviderActivation>),
    /// Exclusively owned authenticated Coinbase Direct account construction.
    CoinbaseDirect(Box<CoinbaseDirectAccountActivation>),
    /// Registered research extraction adapter.
    Research(Box<ActivatedResearchProvider>),
}

impl From<LiveProviderActivation> for ProviderActivationOutcome {
    fn from(value: LiveProviderActivation) -> Self {
        Self::Live(Box::new(value))
    }
}

impl From<CoinbaseDirectAccountActivation> for ProviderActivationOutcome {
    fn from(value: CoinbaseDirectAccountActivation) -> Self {
        Self::CoinbaseDirect(Box::new(value))
    }
}

impl From<ActivatedResearchProvider> for ProviderActivationOutcome {
    fn from(value: ActivatedResearchProvider) -> Self {
        Self::Research(Box::new(value))
    }
}

fn require_surface(
    lease: &ProviderActivationLease,
    expected: &str,
) -> Result<(), ProviderAdapterActivationError> {
    if lease.surface_id().as_str() == expected {
        Ok(())
    } else {
        Err(ProviderAdapterActivationError::SurfaceMismatch)
    }
}

fn require_runtime_lease(
    runtime: &ResearchProviderRuntimeGeneration,
    lease: &ProviderActivationLease,
) -> Result<(), ProviderAdapterActivationError> {
    if runtime.profile() == lease.surface_id()
        && runtime.session_id() == lease.session_id()
        && runtime.capability_revision() == lease.capability_revision()
        && runtime.capability_digest() == lease.capability_digest()
        && runtime.credential_generation() == lease.generation()
        && runtime.secret_reference() == lease.secret_reference()
        && runtime.authority_effective_at() == lease.authority_effective_at()
        && runtime.parent_rights_authorization_evidence() == lease.rights_decision_digest()
    {
        Ok(())
    } else {
        Err(ProviderAdapterActivationError::SourceBinding)
    }
}

fn runtime_lease_is_current(
    onboarding: &ProviderOnboardingMutationAuthority<'_>,
    runtime: &ResearchProviderRuntimeGeneration,
) -> Result<bool, ProviderAdapterActivationError> {
    match onboarding.active_lease(runtime.session_id()) {
        Ok(lease) => Ok(require_runtime_lease(runtime, &lease).is_ok()),
        Err(ProviderOnboardingError::ActivationUnavailable) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn provider_research_rights(
    lease: &ProviderActivationLease,
    source_id: &SourceId,
) -> Result<ResearchRightsAuthority, ProviderAdapterActivationError> {
    if !lease.admits(DataUseOperation::Persist) {
        return Err(ProviderAdapterActivationError::InvalidRights);
    }
    let evidence = lease
        .persistence_evidence()
        .filter(|evidence| !evidence.refresh_required())
        .ok_or(ProviderAdapterActivationError::InvalidRights)?;
    let terms_digest = evidence
        .content_digest()
        .ok_or(ProviderAdapterActivationError::InvalidRights)?;
    let basis = RightsBasis::reviewed_terms(evidence.official_url(), terms_digest)
        .map_err(|_error| ProviderAdapterActivationError::InvalidRights)?;
    ResearchRightsAuthority::try_new(
        source_id.clone(),
        basis,
        lease.rights_decision_digest(),
        lease.verification_expires_at(),
    )
    .map_err(Into::into)
}

fn controlled_local_file_rights(
    lease: &ProviderActivationLease,
    source_id: &SourceId,
    evidence: &market_squawk_data::ImportedUserInputEvidence,
) -> Result<ResearchRightsAuthority, ProviderAdapterActivationError> {
    if !lease.admits(DataUseOperation::Persist) {
        return Err(ProviderAdapterActivationError::InvalidRights);
    }
    ResearchRightsAuthority::try_new(
        source_id.clone(),
        RightsBasis::imported_user_input(evidence.clone()),
        lease.capability_digest(),
        None,
    )
    .map_err(Into::into)
}

fn fred_research_rights(
    lease: &ProviderActivationLease,
    source_id: &SourceId,
    policy: &FredRightsPolicy,
) -> Result<ResearchRightsAuthority, ProviderAdapterActivationError> {
    let authority = policy
        .durable_authority(lease.issued_at())
        .map_err(|_error| ProviderAdapterActivationError::InvalidRights)?;
    let subjects = authority.series().cloned().collect::<Vec<_>>();
    let mut operations = Vec::new();
    for (fred, source) in [
        (FredOperation::Display, SourceOperation::Display),
        (FredOperation::Persist, SourceOperation::Persist),
        (FredOperation::Cache, SourceOperation::Cache),
        (FredOperation::Train, SourceOperation::Train),
        (FredOperation::Redistribute, SourceOperation::Redistribute),
    ] {
        if subjects.iter().all(|series| authority.admits(series, fred)) {
            operations.push(source);
        }
    }
    let basis = RightsBasis::reviewed_terms(
        authority.terms_reference(),
        EvidenceDigest::new(DigestAlgorithm::Sha256, authority.terms_digest().bytes()),
    )
    .map_err(|_error| ProviderAdapterActivationError::InvalidRights)?;
    ResearchRightsAuthority::try_new_scoped(
        source_id.clone(),
        basis,
        lease.rights_decision_digest(),
        EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            authority.authorization_digest().bytes(),
        ),
        authority.expires_at(),
        subjects,
        operations,
    )
    .map_err(Into::into)
}

fn runtime_generation(
    lease: &ProviderActivationLease,
    metadata: market_squawk_sources::SourceMetadata,
    rights: ResearchRightsAuthority,
) -> Result<ResearchProviderRuntimeGeneration, ProviderAdapterActivationError> {
    ResearchProviderRuntimeGeneration::try_new(
        lease.surface_id().clone(),
        lease.session_id(),
        lease.capability_revision(),
        lease.capability_digest(),
        lease.generation(),
        lease.secret_reference().cloned(),
        lease.authority_effective_at(),
        metadata,
        rights,
    )
    .map_err(Into::into)
}
