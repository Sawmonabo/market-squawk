//! Lease-gated construction of production live and research adapters.

mod specs;

use std::fmt;
use std::sync::Arc;

use market_squawk_adapter_bls::{BlsAuthorization, BlsRegistrationKey, BlsSource, BlsSourceConfig};
use market_squawk_adapter_files::FileExtractionSource;
use market_squawk_adapter_fred::{FredApiKey, FredSource};
use market_squawk_adapter_portfolio::PortfolioManifestExtractionSource;
use market_squawk_adapter_sec::{SecContact, SecEdgarSource};
use market_squawk_adapter_treasury::{TreasurySource, TreasurySourceConfig};
use market_squawk_data::RightsBasis;
use market_squawk_domain::{SourceId, SourceIdentifier};
use market_squawk_platform::AppConfig;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::application::{
    ManagedResearchExtractionSource, ProductionResearchIngestCoordinator, ResearchRightsAuthority,
};
use crate::{
    ProductionLiveSourceComposition, ProductionSourceProvider, ProviderActivationLease,
    ProviderOnboardingError, ProviderOnboardingService,
};
use market_squawk_sources::DataUseOperation;

pub use specs::{
    BlsAdapterActivation, FredAdapterActivation, LocalFileAdapterActivation,
    PortfolioAdapterActivation, ProviderAdapterActivationError, ProviderAdapterActivationRequest,
    SecAdapterActivation, TreasuryAdapterActivation,
};

const COINBASE_SURFACE: &str = "coinbase.public-market-data";
const KRAKEN_SURFACE: &str = "kraken.spot-public-market-data";
const SEC_SURFACE: &str = "sec.edgar-public";
const BLS_PUBLIC_SURFACE: &str = "bls.v1-unregistered";
const BLS_REGISTERED_SURFACE: &str = "bls.v2-registered";
const TREASURY_XML_SURFACE: &str = "treasury.daily-rates-xml";
const TREASURY_FISCAL_SURFACE: &str = "treasury.fiscal-data";
const FRED_SURFACE: &str = "fred-alfred.api-v1-v2";
const LOCAL_FILES_SURFACE: &str = "local.files";
const PORTFOLIO_SURFACE: &str = "local.portfolio-imports";

/// Application-owned activation authority shared by CLI, MCP, and local onboarding transports.
pub struct ProviderAdapterActivation {
    onboarding: Arc<ProviderOnboardingService>,
    research: Arc<ProductionResearchIngestCoordinator>,
    app_config: AppConfig,
}

impl ProviderAdapterActivation {
    /// Binds the sole onboarding authority, research coordinator, and validated live configuration.
    #[must_use]
    pub fn new(
        onboarding: Arc<ProviderOnboardingService>,
        research: Arc<ProductionResearchIngestCoordinator>,
        app_config: AppConfig,
    ) -> Self {
        Self {
            onboarding,
            research,
            app_config,
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
    pub async fn activate_ready_profile(
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
    pub fn restore_active_profile(
        &self,
        session_id: Uuid,
        request: ProviderAdapterActivationRequest,
    ) -> Result<ProviderActivationOutcome, ProviderAdapterActivationError> {
        let lease = self.onboarding.activation_lease(session_id)?;
        self.restore_with_lease(lease, request)
    }

    /// Returns whether this process already owns the exact research profile.
    pub(crate) fn is_research_profile_active(
        &self,
        profile: &SourceIdentifier,
    ) -> Result<bool, ProviderAdapterActivationError> {
        self.research
            .is_profile_registered(profile)
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
        let composition = ProductionLiveSourceComposition::try_for_provider(
            self.app_config.clone(),
            routes,
            provider,
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
        let authorization = match lease.surface_id().as_str() {
            BLS_PUBLIC_SURFACE => BlsAuthorization::PublicV1,
            BLS_REGISTERED_SURFACE => {
                let secret = self
                    .onboarding
                    .read_active_secret_for_request(&lease, cancellation)
                    .await?;
                BlsAuthorization::RegisteredV2(BlsRegistrationKey::try_new(
                    secret.expose_secret().to_owned(),
                )?)
            }
            _ => return Err(ProviderAdapterActivationError::SurfaceMismatch),
        };
        let rights = provider_research_rights(&lease, spec.metadata.source_id())?;
        let config =
            BlsSourceConfig::try_new(authorization, spec.series, spec.start_year, spec.end_year)?;
        let source = BlsSource::try_new(spec.metadata, config)?;
        self.register(lease, source, rights)
    }

    fn restore_bls(
        &self,
        lease: ProviderActivationLease,
        spec: BlsAdapterActivation,
    ) -> Result<ActivatedResearchProvider, ProviderAdapterActivationError> {
        let authorization = match lease.surface_id().as_str() {
            BLS_PUBLIC_SURFACE => BlsAuthorization::PublicV1,
            BLS_REGISTERED_SURFACE => {
                return Err(ProviderAdapterActivationError::ExplicitResumeRequired);
            }
            _ => return Err(ProviderAdapterActivationError::SurfaceMismatch),
        };
        let rights = provider_research_rights(&lease, spec.metadata.source_id())?;
        let config =
            BlsSourceConfig::try_new(authorization, spec.series, spec.start_year, spec.end_year)?;
        let source = BlsSource::try_new(spec.metadata, config)?;
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
            ) | (
                TREASURY_XML_SURFACE,
                TreasurySourceConfig::DailyParYieldCurve { .. }
            )
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
            .read_active_secret_for_request(&lease, cancellation)
            .await?;
        let key = FredApiKey::try_new(secret.expose_secret().to_owned())?;
        let rights = provider_research_rights(&lease, spec.metadata.source_id())?;
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
        self.research
            .register_source(profile.clone(), source, rights)?;
        Ok(ActivatedResearchProvider { lease, profile })
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
}

/// Closed result of a ready-profile activation request.
#[derive(Debug)]
pub enum ProviderActivationOutcome {
    /// Validated live connector construction.
    Live(Box<LiveProviderActivation>),
    /// Registered research extraction adapter.
    Research(Box<ActivatedResearchProvider>),
}

impl From<LiveProviderActivation> for ProviderActivationOutcome {
    fn from(value: LiveProviderActivation) -> Self {
        Self::Live(Box::new(value))
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
