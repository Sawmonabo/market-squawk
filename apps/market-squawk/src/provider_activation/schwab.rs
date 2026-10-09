//! Exact read-only Schwab OAuth activation for one account market runtime.

use std::{
    collections::BTreeSet,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_adapter_schwab::{
    AccessTokenAdmission, ParseBounds, ProviderIdentifier, RequestAdmission, RestTransportBounds,
    SchwabCredentialAuthorityBinding, SchwabOAuthAuthorityReceipt, SchwabTransportTelemetry,
    TransientAccessToken,
};
use market_squawk_data::{
    DatasetId, ListingReferenceGenerationReceipt, ListingReferenceReadCapability,
    ListingReferenceRightsState,
};
use market_squawk_domain::{
    AssignmentVerification, DataQuality, EffectiveInterval, IdentifierEntitlement, LiveEventClass,
    Timestamp, VenueId,
};
use market_squawk_sources::{
    ProviderRateAuthority, ProviderRateDeclaration, SCHWAB_MARKET_DATA_SURFACE_ID, SourceMetadata,
};
use tokio_util::sync::CancellationToken;

use crate::application::{
    MarketEventDurableRead, ResearchIngestCompositionError, ResearchProviderRuntimeGeneration,
    SchwabMarketPublicationError, SchwabRestQuoteCurrentRuntimeInput,
    SchwabRestQuotePublicationPackage, SchwabRestQuoteRuntimeBounds, SchwabRestQuoteRuntimeError,
    SchwabRestQuoteSourceEvidence,
};
use crate::live_source::SchwabRestQuoteCurrentSessionInput;
use crate::provider_onboarding::{
    SchwabOAuthMarketAuthority, SchwabOAuthPublicationEpoch, SchwabOAuthReceiptCurrentness,
};
use crate::{ProviderActivationLease, ProviderOnboardingError};

use super::account::{
    ProviderAccountActivationError, ProviderAccountBinding, ProviderAccountRuntimeAuthority,
    ProviderAccountRuntimeCurrentness, ProviderMarketAccount,
};
use super::{
    MarketDataInstrumentBinding, MarketInstrumentReferenceBinding,
    MarketReferenceIdentityApprovalV1, MarketReferenceIdentityAuthority, ProviderAdapterActivation,
    SchwabQuoteReferenceBinding,
};

const SCHWAB_QUOTE_SOURCE_ID: &str = "schwab-trader-api";
const SCHWAB_QUOTE_PROVIDER: &str = "schwab-trader-api";
const SCHWAB_QUOTE_PRODUCT: &str = "schwab-rest";
const SCHWAB_QUOTE_CHANNEL: &str = "schwab-rest-quotes";
const SCHWAB_QUOTE_VENUE: &str = "schwab";
const SCHWAB_QUOTE_MAXIMUM_SYMBOLS: usize = 50;
const SCHWAB_QUOTE_MAXIMUM_REQUEST_BYTES: usize = 16 * 1024;
const SCHWAB_QUOTE_MAXIMUM_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const SCHWAB_QUOTE_MAXIMUM_JSON_NODES: usize = 256 * 1024;
const SCHWAB_QUOTE_MAXIMUM_JSON_DEPTH: usize = 64;
const SCHWAB_QUOTE_MAXIMUM_UNKNOWN_FIELDS: usize = 512;
const SCHWAB_QUOTE_MAXIMUM_UNKNOWN_BYTES: usize = 512 * 1024;
const SCHWAB_QUOTE_MAXIMUM_HEADERS: usize = 64;
const SCHWAB_QUOTE_MAXIMUM_HEADER_BYTES: usize = 64 * 1024;
const SCHWAB_QUOTE_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const SCHWAB_QUOTE_READ_TIMEOUT: Duration = Duration::from_secs(15);
const SCHWAB_QUOTE_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const SCHWAB_QUOTE_MINIMUM_TOKEN_LIFETIME: Duration = Duration::from_secs(60);
const SCHWAB_QUOTE_FRESHNESS_MARGIN_DIVISOR: u64 = 2;

/// Non-clone owner of one callable Schwab read-only market-data epoch.
///
/// It retains the exact prepared or active onboarding lease, protected OAuth authorization,
/// account-lifetime authority, and shared provider-rate authority. It exposes no
/// account, position, transaction, order, or money-movement operation.
pub struct SchwabMarketDataAccountActivation {
    authority: Arc<ProviderAccountRuntimeAuthority>,
    oauth: SchwabOAuthMarketAuthority,
    oauth_authorization: SchwabOAuthAuthorityReceipt,
    market_hours_demand: tokio::sync::Mutex<()>,
    streamer_owner: Arc<tokio::sync::Mutex<()>>,
    preparation_complete: Arc<AtomicBool>,
}

/// One selected current source on the sole account group; REST and Streamer never both own it.
#[derive(Debug)]
pub(crate) enum PreparedSchwabMarketRuntimeStart {
    Rest(PreparedSchwabRestMarketRuntimeStart),
    Streamer(super::schwab_streamer::PreparedSchwabStreamerMarketRuntimeStart),
}
impl PreparedSchwabMarketRuntimeStart {
    /// Account lifecycle identity is shared; each transport retains its own source profile.
    pub(crate) fn has_expected_generation_profile(&self) -> bool {
        let expected = match self {
            Self::Rest(_) => super::schwab_quote_metadata::PROFILE,
            Self::Streamer(_) => super::schwab_quote_metadata::STREAMER_PROFILE,
        };
        self.generation().profile().as_str() == expected
    }

    pub(crate) fn account_owner(&self) -> Arc<SchwabMarketDataAccountActivation> {
        match self {
            Self::Rest(value) => value.account_owner(),
            Self::Streamer(value) => Arc::clone(&value.activation),
        }
    }
    pub(crate) fn generation(&self) -> &ResearchProviderRuntimeGeneration {
        match self {
            Self::Rest(value) => value.generation(),
            Self::Streamer(value) => &value.generation,
        }
    }
    pub(crate) fn durable_read(&self) -> &MarketEventDurableRead {
        match self {
            Self::Rest(value) => value.durable_read(),
            Self::Streamer(value) => &value.publication.durable_read,
        }
    }
    pub(crate) fn activation_lease(&self) -> &ProviderActivationLease {
        match self {
            Self::Rest(value) => value.activation_lease(),
            Self::Streamer(value) => value.activation.lease(),
        }
    }
    pub(crate) fn currentness(&self) -> ProviderAccountRuntimeCurrentness {
        match self {
            Self::Rest(value) => value.currentness(),
            Self::Streamer(value) => value.activation.currentness(),
        }
    }
    pub(crate) fn metadata(&self) -> &SourceMetadata {
        match self {
            Self::Rest(value) => value.metadata(),
            Self::Streamer(value) => value.generation.metadata(),
        }
    }
    pub(crate) fn venue_id(&self) -> &VenueId {
        match self {
            Self::Rest(value) => value.venue_id(),
            Self::Streamer(value) => &value.venue,
        }
    }
    pub(crate) fn bindings(
        &self,
    ) -> &[(
        SchwabQuoteReferenceBinding,
        Option<MarketReferenceIdentityApprovalV1>,
    )] {
        match self {
            Self::Rest(value) => value.bindings(),
            Self::Streamer(value) => &value.bindings,
        }
    }
    pub(crate) fn display_bindings(&self) -> &[MarketDataInstrumentBinding] {
        match self {
            Self::Rest(value) => value.display_bindings(),
            Self::Streamer(value) => &value.display_bindings,
        }
    }
}

/// One-use upstream package for the exact registered Schwab current-quote generation.
///
/// Construction retains the single account activation and sole durable publication package.
/// Internal shared ownership permits independent family demands without copying OAuth or
/// publication authority; the native Streamer claim remains exclusive.
pub(crate) struct PreparedSchwabRestMarketRuntimeStart {
    activation: Arc<SchwabMarketDataAccountActivation>,
    provider_rate: ProviderRateAuthority,
    generation: ResearchProviderRuntimeGeneration,
    evidence: SchwabRestQuoteSourceEvidence,
    bindings: Vec<(
        SchwabQuoteReferenceBinding,
        Option<MarketReferenceIdentityApprovalV1>,
    )>,
    display_bindings: Box<[MarketDataInstrumentBinding]>,
    reference_identity: Option<MarketReferenceIdentityAuthority>,
    listing_reference: Option<ListingReferenceReadCapability>,
    nasdaq_generation: Option<ListingReferenceGenerationReceipt>,
    bounds: SchwabRestQuoteRuntimeBounds,
    telemetry: SchwabTransportTelemetry,
    publication: SchwabRestQuotePublicationPackage,
    request_timeout: Duration,
    poll_interval: Duration,
}

impl PreparedSchwabRestMarketRuntimeStart {
    pub(crate) fn account_owner(&self) -> Arc<SchwabMarketDataAccountActivation> {
        Arc::clone(&self.activation)
    }

    /// Returns the exact registered provider-runtime generation retained by this package.
    pub(crate) const fn generation(&self) -> &ResearchProviderRuntimeGeneration {
        &self.generation
    }

    /// Returns the paired provider-neutral durable read before the package is consumed.
    pub(crate) const fn durable_read(&self) -> &MarketEventDurableRead {
        self.publication.durable_read()
    }

    pub(crate) fn activation_lease(&self) -> &ProviderActivationLease {
        self.activation.lease()
    }

    pub(crate) fn currentness(&self) -> ProviderAccountRuntimeCurrentness {
        self.activation.currentness()
    }

    pub(crate) const fn metadata(&self) -> &SourceMetadata {
        self.evidence.metadata()
    }

    pub(crate) const fn venue_id(&self) -> &VenueId {
        self.evidence.venue_id()
    }

    pub(crate) fn bindings(
        &self,
    ) -> &[(
        SchwabQuoteReferenceBinding,
        Option<MarketReferenceIdentityApprovalV1>,
    )] {
        &self.bindings
    }

    pub(crate) fn display_bindings(&self) -> &[MarketDataInstrumentBinding] {
        &self.display_bindings
    }

    /// Joins the registry-minted current session and lifecycle to the already prepared upstream
    /// authorities without reconstructing source evidence, controls, or durable publication.
    pub(crate) fn into_runtime_input(
        self,
        current: SchwabRestQuoteCurrentSessionInput,
        lifecycle: CancellationToken,
    ) -> SchwabRestQuoteCurrentRuntimeInput {
        let (durable, durable_writer) = self.publication.into_runtime_parts();
        SchwabRestQuoteCurrentRuntimeInput::new(
            self.activation,
            self.provider_rate,
            self.evidence,
            self.bindings,
            self.reference_identity,
            self.listing_reference,
            self.nasdaq_generation,
            self.bounds,
            self.telemetry,
            durable,
            durable_writer,
            current,
            self.request_timeout,
            self.poll_interval,
            lifecycle,
        )
    }
}

impl std::fmt::Debug for PreparedSchwabRestMarketRuntimeStart {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedSchwabRestMarketRuntimeStart")
            .field("generation", &self.generation)
            .field("instrument_count", &self.bindings.len())
            .field("oauth", &"[PROTECTED TOKEN AUTHORITY]")
            .field("publication", &"[EXACT DURABLE AUTHORITY]")
            .finish()
    }
}

impl SchwabMarketDataAccountActivation {
    /// One actual account owns all family demands; this is not a copied account proof.
    pub(crate) fn market_hours_demand(&self) -> &tokio::sync::Mutex<()> {
        &self.market_hours_demand
    }

    pub(crate) fn claim_streamer(
        &self,
    ) -> Result<tokio::sync::OwnedMutexGuard<()>, market_squawk_adapter_schwab::SchwabTransportError>
    {
        Arc::clone(&self.streamer_owner)
            .try_lock_owned()
            .map_err(|_| market_squawk_adapter_schwab::SchwabTransportError::InvalidConfiguration)
    }

    pub fn lease(&self) -> &ProviderActivationLease {
        self.authority.lease()
    }

    pub fn account_binding(&self) -> &ProviderAccountBinding {
        self.authority.binding()
    }

    /// Uses the same account-qualified policy for metadata, validation, and request admission.
    pub(crate) fn provider_rate_declaration(
        &self,
    ) -> Result<ProviderRateDeclaration, SchwabMarketDataActivationError> {
        ProviderRateDeclaration::try_for_authorization_subject(
            self.lease()
                .provider_budget_policy()
                .cloned()
                .ok_or(SchwabMarketDataActivationError::AuthorityMismatch)?,
            self.account_binding().subject(),
        )
        .map_err(|_| SchwabMarketDataActivationError::AuthorityMismatch)
    }

    pub(crate) fn oauth_receipt_currentness(&self) -> SchwabOAuthReceiptCurrentness {
        self.oauth.receipt_currentness()
    }

    pub(crate) async fn current_oauth_receipt(
        &self,
    ) -> Result<SchwabOAuthAuthorityReceipt, SchwabMarketDataActivationError> {
        self.authority.require_current().await?;
        let receipt = self.oauth.current_receipt().await?;
        self.validate_oauth_authorization(receipt)?;
        self.authority.require_current().await?;
        Ok(receipt)
    }

    /// Keeps runtime ownership on the configured application and original OAuth grant. Scope and
    /// token clocks remain actual request evidence, not proof that a data family is available.
    pub(crate) fn validate_oauth_authorization(
        &self,
        receipt: SchwabOAuthAuthorityReceipt,
    ) -> Result<(), SchwabMarketDataActivationError> {
        if receipt.credential_authority() != self.oauth_authorization.credential_authority()
            || receipt.authorization_generation()
                != self.oauth_authorization.authorization_generation()
        {
            return Err(SchwabMarketDataActivationError::AuthorityMismatch);
        }
        Ok(())
    }

    pub(crate) fn currentness(&self) -> ProviderAccountRuntimeCurrentness {
        self.authority.currentness()
    }

    /// Startup admission for the unpublished runtime; promotion permanently requires Active.
    pub(crate) fn runtime_currentness(&self) -> ProviderAccountRuntimeCurrentness {
        self.authority
            .runtime_currentness(Arc::clone(&self.preparation_complete))
    }

    /// Called only under onboarding mutation ownership after exact activation is committed,
    /// before the ready runtime is inserted into the public registry.
    pub(crate) fn seal_runtime_preparation(&self) {
        self.preparation_complete.store(true, Ordering::Release);
    }

    pub(crate) async fn runtime_oauth_receipt(
        &self,
    ) -> Result<SchwabOAuthAuthorityReceipt, SchwabMarketDataActivationError> {
        self.runtime_currentness()
            .acquire_publication_authority()
            .await?;
        let receipt = self.oauth.current_receipt().await?;
        self.validate_oauth_authorization(receipt)?;
        self.runtime_currentness()
            .acquire_publication_authority()
            .await?;
        Ok(receipt)
    }

    pub(crate) async fn require_runtime_current(
        &self,
    ) -> Result<(), SchwabMarketDataActivationError> {
        self.runtime_oauth_receipt().await.map(|_| ())
    }

    /// Acquires an exact token epoch for the private startup or the subsequently active runtime.
    pub(crate) async fn acquire_runtime_publication_attempt(
        &self,
    ) -> Result<(TransientAccessToken, SchwabOAuthPublicationEpoch), SchwabMarketDataActivationError>
    {
        self.runtime_currentness()
            .acquire_publication_authority()
            .await?;
        let (token, epoch) = self.oauth.acquire_publication_attempt().await?;
        self.validate_oauth_authorization(epoch.receipt())?;
        self.runtime_currentness()
            .acquire_publication_authority()
            .await?;
        Ok((token, epoch))
    }

    pub async fn require_current(&self) -> Result<(), SchwabMarketDataActivationError> {
        self.current_oauth_receipt().await.map(|_| ())
    }

    /// Acquires one exact token/publication attempt behind the serialized OAuth barrier.
    ///
    /// A protected refresh may advance the access-token generation within the same authorization.
    /// The original grant and application credentials retain runtime ownership; the returned
    /// epoch binds the exact current token for dispatch and publication.
    pub(crate) async fn acquire_publication_attempt(
        &self,
    ) -> Result<(TransientAccessToken, SchwabOAuthPublicationEpoch), SchwabMarketDataActivationError>
    {
        self.authority.require_current().await?;
        let (token, epoch) = self.oauth.acquire_publication_attempt().await?;
        self.validate_oauth_authorization(epoch.receipt())?;
        Ok((token, epoch))
    }

    /// Revalidates the exact current source, definition, reference, and provider-symbol binding.
    ///
    /// Preparation and runtime start share this boundary so a package cannot outlive a canonical
    /// identity interval or silently substitute a provider symbol after it was prepared.
    pub(crate) fn validate_current_quote_bindings(
        &self,
        metadata: &SourceMetadata,
        bindings: &[(
            SchwabQuoteReferenceBinding,
            Option<MarketReferenceIdentityApprovalV1>,
        )],
        nasdaq_generation: Option<&ListingReferenceGenerationReceipt>,
        at: Timestamp,
        maximum: usize,
        require_exact_coverage: bool,
    ) -> Result<(), SchwabMarketDataActivationError> {
        let expected_budget = self.provider_rate_declaration()?;
        if maximum == 0
            || maximum > SCHWAB_QUOTE_MAXIMUM_SYMBOLS
            || !self.account_binding().validates_metadata(metadata)
            || !matches!(
                metadata.source_id().as_str(),
                SCHWAB_QUOTE_SOURCE_ID | super::schwab_quote_metadata::STREAMER_SOURCE
            )
            || metadata.provider().as_str() != SCHWAB_QUOTE_PROVIDER
            || metadata.budget_policy() != Some(expected_budget.policy())
            || !metadata.is_effective_at(at)
            || validate_exact_schwab_quote_bindings(
                bindings,
                metadata,
                nasdaq_generation,
                at,
                maximum,
                require_exact_coverage,
            )
            .is_err()
        {
            return Err(SchwabMarketDataActivationError::AuthorityMismatch);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn acquire_test_publication_attempt(
        oauth: &SchwabOAuthMarketAuthority,
    ) -> Result<(TransientAccessToken, SchwabOAuthPublicationEpoch), SchwabMarketDataActivationError>
    {
        let (token, epoch) = oauth.acquire_publication_attempt().await?;
        Ok((token, epoch))
    }
}

impl std::fmt::Debug for SchwabMarketDataAccountActivation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchwabMarketDataAccountActivation")
            .field("authority", &self.authority)
            .field("oauth", &"[PROTECTED TOKEN AUTHORITY]")
            .field(
                "authorization_generation",
                &self.oauth_authorization.authorization_generation(),
            )
            .finish()
    }
}

impl ProviderAdapterActivation {
    /// Claims the sole native Streamer executor on the retained account owner.
    #[allow(
        clippy::too_many_arguments,
        reason = "account and bounded native inputs remain explicit"
    )]
    pub(crate) async fn prepare_schwab_streamer(
        &self,
        activation: Arc<SchwabMarketDataAccountActivation>,
        services: BTreeSet<market_squawk_adapter_schwab::MarketDataService>,
        control: Arc<dyn market_squawk_adapter_schwab::SchwabStreamerConnectionControlSource>,
        admission: market_squawk_adapter_schwab::StreamerAdmission,
        bounds: market_squawk_adapter_schwab::StreamerTransportBounds,
        parse: ParseBounds,
        token_admission: AccessTokenAdmission,
        telemetry: SchwabTransportTelemetry,
    ) -> Result<
        crate::provider_rate::GovernedSchwabStreamer,
        market_squawk_adapter_schwab::SchwabTransportError,
    > {
        crate::provider_rate::GovernedSchwabStreamer::try_new(
            activation,
            &self.provider_rate,
            services,
            control,
            admission,
            bounds,
            parse,
            token_admission,
            telemetry,
        )
        .await
    }

    /// Activates a configured application with its existing protected OAuth authorization.
    pub(crate) async fn activate_schwab_market_data_account(
        &self,
        lease: ProviderActivationLease,
        oauth: SchwabOAuthMarketAuthority,
        cancellation: CancellationToken,
    ) -> Result<SchwabMarketDataAccountActivation, SchwabMarketDataActivationError> {
        if cancellation.is_cancelled() {
            return Err(SchwabMarketDataActivationError::Cancelled);
        }
        if oauth.session_id() != lease.session_id() {
            return Err(SchwabMarketDataActivationError::AuthorityMismatch);
        }
        let credential = SchwabCredentialAuthorityBinding::try_from_application_credential(
            lease
                .secret_reference()
                .ok_or(SchwabMarketDataActivationError::AuthorityMismatch)?,
        )
        .map_err(|_| SchwabMarketDataActivationError::AuthorityMismatch)?;
        let current = oauth.current_receipt().await?;
        if cancellation.is_cancelled() {
            return Err(SchwabMarketDataActivationError::Cancelled);
        }
        if current.credential_authority() != credential
            || lease.generation() != Some(credential.application_credential_generation())
        {
            return Err(SchwabMarketDataActivationError::AuthorityMismatch);
        }
        let authority = Arc::new(
            ProviderAccountRuntimeAuthority::try_acquire_prepared_or_active(
                ProviderMarketAccount::SchwabMarketData,
                lease,
                Arc::clone(&self.onboarding),
                &self.app_config,
                self.provider_rate.clone(),
            )?,
        );
        let activation = SchwabMarketDataAccountActivation {
            authority,
            oauth,
            oauth_authorization: current,
            market_hours_demand: tokio::sync::Mutex::new(()),
            streamer_owner: Arc::new(tokio::sync::Mutex::new(())),
            preparation_complete: Arc::new(AtomicBool::new(false)),
        };
        activation.require_runtime_current().await?;
        Ok(activation)
    }

    /// Prepares the sole current-quote start package after successful Schwab account activation.
    ///
    /// The caller supplies only a generation already registered by the application research
    /// authority and a bounded set minted from canonical definition/reference capabilities. This
    /// boundary revalidates both against the configured authority and source metadata before it
    /// binds durable publication. It never derives canonical identity from a ticker or provider
    /// response.
    pub(crate) async fn prepare_schwab_market_runtime_start(
        &self,
        activation: Arc<SchwabMarketDataAccountActivation>,
        generation: ResearchProviderRuntimeGeneration,
        instruments: Vec<SchwabQuoteReferenceBinding>,
        display_bindings: Vec<MarketDataInstrumentBinding>,
        reference_identity: Option<MarketReferenceIdentityAuthority>,
        listing_reference: Option<ListingReferenceReadCapability>,
        identity_approvals: Vec<MarketReferenceIdentityApprovalV1>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedSchwabMarketRuntimeStart, SchwabMarketRuntimeStartError> {
        if cancellation.is_cancelled() || Instant::now() >= deadline {
            return Err(SchwabMarketRuntimeStartError::Cancelled);
        }
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(SchwabMarketRuntimeStartError::Cancelled);
            }
            current = activation.require_runtime_current() => current?,
        }
        let now = system_timestamp()?;
        let metadata = generation.metadata();
        let expected_budget = activation.provider_rate_declaration()?;
        if generation.profile().as_str() != SCHWAB_MARKET_DATA_SURFACE_ID
            || super::require_runtime_lease(&generation, activation.lease()).is_err()
            || !activation.account_binding().validates_metadata(metadata)
            || metadata.source_id().as_str() != SCHWAB_QUOTE_SOURCE_ID
            || metadata.provider().as_str() != SCHWAB_QUOTE_PROVIDER
            || metadata.quality_ceiling() != DataQuality::DirectUnverified
            || metadata.budget_policy() != Some(expected_budget.policy())
            || !metadata.is_effective_at(now)
        {
            return Err(SchwabMarketRuntimeStartError::AuthorityMismatch);
        }
        let registered = self
            .research
            .provider_runtime_generation(generation.profile())?
            .ok_or(SchwabMarketRuntimeStartError::GenerationUnavailable)?;
        if registered != generation {
            return Err(SchwabMarketRuntimeStartError::GenerationUnavailable);
        }

        let live = metadata
            .coverage()
            .live()
            .ok_or(SchwabMarketRuntimeStartError::SourceEvidence)?;
        let venue = VenueId::try_from(SCHWAB_QUOTE_VENUE)
            .map_err(|_error| SchwabMarketRuntimeStartError::SourceEvidence)?;
        if live.provider_product().as_source_identifier().as_str() != SCHWAB_QUOTE_PRODUCT
            || live.provider_channel().as_source_identifier().as_str() != SCHWAB_QUOTE_CHANNEL
            || live.rule_for(LiveEventClass::Quote, None).is_none()
            || !metadata.coverage().topology().contains_venue(&venue)
        {
            return Err(SchwabMarketRuntimeStartError::SourceEvidence);
        }
        let nasdaq_generation = selected_nasdaq_generation(&instruments)?;
        match (&nasdaq_generation, &reference_identity, &listing_reference) {
            (Some(expected), Some(_identity), Some(reader)) => {
                let current = reader
                    .current(deadline, &cancellation)
                    .map_err(|_error| SchwabMarketRuntimeStartError::IdentityResolutionRequired)?
                    .ok_or(SchwabMarketRuntimeStartError::IdentityResolutionRequired)?;
                if &current != expected {
                    return Err(SchwabMarketRuntimeStartError::IdentityResolutionRequired);
                }
            }
            (None, None, None) if identity_approvals.is_empty() => {}
            _ => return Err(SchwabMarketRuntimeStartError::IdentityResolutionRequired),
        }
        let bindings = exact_schwab_quote_bindings(
            instruments,
            identity_approvals,
            metadata,
            nasdaq_generation.as_ref(),
            now,
        )?;
        validate_schwab_display_bindings(&bindings, &display_bindings, now)?;
        let bounds = schwab_quote_runtime_bounds()?;
        let oauth = activation.oauth_receipt_currentness();
        let oauth_receipt = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(SchwabMarketRuntimeStartError::Cancelled);
            }
            receipt = activation.runtime_oauth_receipt() => receipt?,
        };
        let evidence = SchwabRestQuoteSourceEvidence::try_new(metadata.clone(), venue)?;
        let analytical_dataset = DatasetId::try_from(super::MARKET_EVENT_ANALYTICAL_DATASET)
            .map_err(|_error| SchwabMarketRuntimeStartError::InvalidControls)?;
        let publication = self
            .research_mutation
            .bind_schwab_rest_quote_publication_package(
                &generation,
                oauth,
                oauth_receipt,
                analytical_dataset,
                SCHWAB_QUOTE_REQUEST_TIMEOUT,
            )?;
        if cancellation.is_cancelled() {
            return Err(SchwabMarketRuntimeStartError::Cancelled);
        }
        let poll_interval = schwab_quote_poll_interval(metadata, SCHWAB_QUOTE_REQUEST_TIMEOUT)?;
        Ok(PreparedSchwabMarketRuntimeStart::Rest(
            PreparedSchwabRestMarketRuntimeStart {
                activation,
                provider_rate: self.provider_rate.clone(),
                generation,
                evidence,
                bindings,
                display_bindings: display_bindings.into_boxed_slice(),
                reference_identity,
                listing_reference,
                nasdaq_generation,
                bounds,
                telemetry: SchwabTransportTelemetry::default(),
                publication,
                request_timeout: SCHWAB_QUOTE_REQUEST_TIMEOUT,
                poll_interval,
            },
        ))
    }
}

pub(super) fn exact_schwab_quote_bindings(
    instruments: Vec<SchwabQuoteReferenceBinding>,
    identity_approvals: Vec<MarketReferenceIdentityApprovalV1>,
    metadata: &SourceMetadata,
    nasdaq_generation: Option<&ListingReferenceGenerationReceipt>,
    at: Timestamp,
) -> Result<
    Vec<(
        SchwabQuoteReferenceBinding,
        Option<MarketReferenceIdentityApprovalV1>,
    )>,
    SchwabMarketRuntimeStartError,
> {
    let bindings = &instruments;
    let mut retained = Vec::new();
    retained
        .try_reserve_exact(bindings.len())
        .map_err(|_error| SchwabMarketRuntimeStartError::InvalidControls)?;
    for binding in bindings {
        let approval = match binding.reference() {
            MarketInstrumentReferenceBinding::NasdaqListing(_) => {
                let mut matches = identity_approvals
                    .iter()
                    .filter(|approval| approval.instrument_id() == binding.instrument_id());
                let approval = matches
                    .next()
                    .cloned()
                    .ok_or(SchwabMarketRuntimeStartError::IdentityResolutionRequired)?;
                if matches.next().is_some() {
                    return Err(SchwabMarketRuntimeStartError::IdentityResolutionRequired);
                }
                Some(approval)
            }
            MarketInstrumentReferenceBinding::AssignedExternalIdentifier(_) => None,
        };
        retained.push((binding.clone(), approval));
    }
    let retained_approvals = retained
        .iter()
        .filter(|(_, approval)| approval.is_some())
        .count();
    if retained_approvals != identity_approvals.len() {
        return Err(SchwabMarketRuntimeStartError::IdentityResolutionRequired);
    }
    validate_exact_schwab_quote_bindings(
        &retained,
        metadata,
        nasdaq_generation,
        at,
        SCHWAB_QUOTE_MAXIMUM_SYMBOLS,
        true,
    )?;
    Ok(retained)
}

pub(super) fn validate_schwab_display_bindings(
    strict: &[(
        SchwabQuoteReferenceBinding,
        Option<MarketReferenceIdentityApprovalV1>,
    )],
    display: &[MarketDataInstrumentBinding],
    at: Timestamp,
) -> Result<(), SchwabMarketRuntimeStartError> {
    if strict.len() != display.len()
        || strict.iter().any(|(binding, _approval)| {
            display
                .iter()
                .filter(|candidate| strict_and_display_definition_match(binding, candidate, at))
                .count()
                != 1
        })
        || display.iter().any(|candidate| {
            strict
                .iter()
                .filter(|(binding, _approval)| {
                    strict_and_display_definition_match(binding, candidate, at)
                })
                .count()
                != 1
        })
    {
        return Err(SchwabMarketRuntimeStartError::CanonicalIdentity);
    }
    Ok(())
}

fn strict_and_display_definition_match(
    strict: &SchwabQuoteReferenceBinding,
    display: &MarketDataInstrumentBinding,
    at: Timestamp,
) -> bool {
    let record = strict.canonical_record();
    let definition = record.definition();
    record.published_at() <= at
        && interval_contains(definition.effective_interval(), at)
        && display.instrument_id() == strict.instrument_id()
        && display.instrument_id() == definition.instrument_id()
        && display.provisional_subscription_symbol() == strict.provider_symbol()
        && display.asset_class() == strict.definition().asset_class()
        && display.asset_class() == definition.asset_class()
        && display.priority() == strict.priority()
        && display.definition_reference_evidence() == definition.reference_evidence()
        && display.definition_effective() == definition.effective_interval()
        && display.definition_revision_digest() == record.revision_digest()
}

fn validate_exact_schwab_quote_bindings(
    bindings: &[(
        SchwabQuoteReferenceBinding,
        Option<MarketReferenceIdentityApprovalV1>,
    )],
    metadata: &SourceMetadata,
    nasdaq_generation: Option<&ListingReferenceGenerationReceipt>,
    at: Timestamp,
    maximum: usize,
    require_exact_coverage: bool,
) -> Result<(), SchwabMarketRuntimeStartError> {
    if bindings.is_empty() || maximum == 0 || bindings.len() > maximum {
        return Err(SchwabMarketRuntimeStartError::CanonicalIdentity);
    }
    let mut instrument_ids = BTreeSet::new();
    let mut provider_symbols = BTreeSet::new();
    for (binding, approval) in bindings {
        let canonical_definition = binding.canonical_record();
        let identity = binding.provider_identity();
        let reference = binding.reference();
        if canonical_definition.published_at() > at
            || !interval_contains(canonical_definition.definition().effective_interval(), at)
            || !reference_is_current(binding, reference, approval.as_ref(), nasdaq_generation, at)
            || identity.source_id().as_str()
                != super::schwab_quote_binding::SCHWAB_INSTRUMENT_REFERENCE_SOURCE
            || identity.instrument_id() != binding.instrument_id()
            || binding.definition().provider_identity_at(
                identity.source_id(),
                identity.provider_instrument_id(),
                at,
            ) != Some(identity)
            || !metadata
                .coverage()
                .asset_classes()
                .contains(&binding.definition().asset_class())
            || ProviderIdentifier::try_new(binding.provider_symbol().to_owned()).is_err()
            || !instrument_ids.insert(binding.instrument_id())
            || !provider_symbols.insert(binding.provider_symbol().to_owned())
        {
            return Err(SchwabMarketRuntimeStartError::CanonicalIdentity);
        }
    }
    let covered = metadata.coverage().instruments().instruments();
    if require_exact_coverage
        && (covered.len() != instrument_ids.len()
            || covered
                .iter()
                .any(|instrument| !instrument_ids.contains(instrument)))
    {
        return Err(SchwabMarketRuntimeStartError::CanonicalIdentity);
    }
    Ok(())
}

fn reference_is_current(
    binding: &SchwabQuoteReferenceBinding,
    reference: &MarketInstrumentReferenceBinding,
    approval: Option<&MarketReferenceIdentityApprovalV1>,
    nasdaq_generation: Option<&ListingReferenceGenerationReceipt>,
    at: Timestamp,
) -> bool {
    let canonical_record = binding.canonical_record();
    let canonical_definition = canonical_record.definition();
    match reference {
        MarketInstrumentReferenceBinding::NasdaqListing(listing) => {
            let Some(approval) = approval else {
                return false;
            };
            listing.generation().rights_state() == ListingReferenceRightsState::AdmittedScoped
                && !listing.is_test_issue()
                && listing.effective_at() <= at
                && listing.generation().published_at() <= at
                && nasdaq_generation == Some(listing.generation())
                && approval.request().provider_instrument_id().as_str() == listing.provider_symbol()
                && approval.request().venue_id() == listing.listing_venue()
                && approval.instrument_id() == binding.instrument_id()
                && approval.asset_class() == canonical_definition.asset_class()
                && approval.quote_currency() == canonical_definition.quote_currency()
                && approval.definition_revision_digest() == canonical_record.revision_digest()
                && approval.definition_reference_evidence()
                    == canonical_definition.reference_evidence()
                && approval.quote_currency_evidence()
                    == canonical_definition.quote_currency_evidence()
                && approval.listing_payload_evidence() == listing.source_file().payload_evidence()
                && approval.listing_source_timestamp() == listing.effective_at()
                && approval.listing_observed_at() == listing.source_file().received_at()
                && approval.evaluated_at() < approval.expires_at()
                && at < approval.expires_at()
        }
        MarketInstrumentReferenceBinding::AssignedExternalIdentifier(record) => {
            approval.is_none()
                && record.assignment_verification() == AssignmentVerification::VerifiedAssigned
                && record.rights_policy().entitlement()
                    != IdentifierEntitlement::UnknownOrRestricted
                && interval_contains(record.validity(), at)
        }
    }
}

pub(super) fn selected_nasdaq_generation(
    bindings: &[SchwabQuoteReferenceBinding],
) -> Result<Option<ListingReferenceGenerationReceipt>, SchwabMarketRuntimeStartError> {
    let mut selected = None;
    for binding in bindings {
        let MarketInstrumentReferenceBinding::NasdaqListing(listing) = binding.reference() else {
            continue;
        };
        match &selected {
            Some(current) if current != listing.generation() => {
                return Err(SchwabMarketRuntimeStartError::IdentityResolutionRequired);
            }
            Some(_) => {}
            None => selected = Some(listing.generation().clone()),
        }
    }
    Ok(selected)
}

fn interval_contains(interval: EffectiveInterval, at: Timestamp) -> bool {
    interval.starts_at() <= at && interval.ends_at().is_none_or(|end| at < end)
}

fn schwab_quote_poll_interval(
    metadata: &SourceMetadata,
    request_timeout: Duration,
) -> Result<Duration, SchwabMarketRuntimeStartError> {
    let freshness = metadata.freshness_policy();
    let freshness_ceiling = freshness
        .max_connection_idle_nanos()
        .min(freshness.max_transport_age_nanos())
        .min(freshness.max_source_age_nanos())
        .min(freshness.max_market_age_nanos());
    let response_margin = freshness_ceiling
        .checked_div(SCHWAB_QUOTE_FRESHNESS_MARGIN_DIVISOR)
        .ok_or(SchwabMarketRuntimeStartError::InvalidControls)?;
    let request_timeout_nanos = u64::try_from(request_timeout.as_nanos())
        .map_err(|_error| SchwabMarketRuntimeStartError::InvalidControls)?;
    let poll_nanos = response_margin
        .checked_sub(request_timeout_nanos)
        .filter(|value| *value > 0)
        .ok_or(SchwabMarketRuntimeStartError::InvalidControls)?;
    Ok(Duration::from_nanos(poll_nanos))
}

fn schwab_quote_runtime_bounds()
-> Result<SchwabRestQuoteRuntimeBounds, SchwabMarketRuntimeStartError> {
    let request_bytes = nonzero(SCHWAB_QUOTE_MAXIMUM_REQUEST_BYTES)?;
    let symbols = nonzero(SCHWAB_QUOTE_MAXIMUM_SYMBOLS)?;
    let response_bytes = nonzero(SCHWAB_QUOTE_MAXIMUM_RESPONSE_BYTES)?;
    let bounds = SchwabRestQuoteRuntimeBounds {
        request_admission: RequestAdmission::new(request_bytes, symbols),
        transport: RestTransportBounds::try_new(
            SCHWAB_QUOTE_CONNECT_TIMEOUT,
            SCHWAB_QUOTE_READ_TIMEOUT,
            SCHWAB_QUOTE_REQUEST_TIMEOUT,
            response_bytes,
            nonzero(SCHWAB_QUOTE_MAXIMUM_HEADERS)?,
            nonzero(SCHWAB_QUOTE_MAXIMUM_HEADER_BYTES)?,
        )
        .map_err(|_error| SchwabMarketRuntimeStartError::InvalidControls)?,
        parse: ParseBounds::new(
            response_bytes,
            symbols,
            nonzero(SCHWAB_QUOTE_MAXIMUM_JSON_NODES)?,
            nonzero(SCHWAB_QUOTE_MAXIMUM_JSON_DEPTH)?,
            SCHWAB_QUOTE_MAXIMUM_UNKNOWN_FIELDS,
            SCHWAB_QUOTE_MAXIMUM_UNKNOWN_BYTES,
        ),
        token: AccessTokenAdmission::new(request_bytes, SCHWAB_QUOTE_MINIMUM_TOKEN_LIFETIME),
    };
    Ok(bounds)
}

fn nonzero(value: usize) -> Result<NonZeroUsize, SchwabMarketRuntimeStartError> {
    NonZeroUsize::new(value).ok_or(SchwabMarketRuntimeStartError::InvalidControls)
}

pub(super) fn system_timestamp() -> Result<Timestamp, SchwabMarketRuntimeStartError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| SchwabMarketRuntimeStartError::Clock)?;
    let nanos =
        i64::try_from(elapsed.as_nanos()).map_err(|_error| SchwabMarketRuntimeStartError::Clock)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

/// Fail-closed construction error for the one-use Schwab current-market start package.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SchwabMarketRuntimeStartError {
    #[error("Schwab market runtime preparation was cancelled")]
    Cancelled,
    #[error("Schwab market runtime authority does not match the configured OAuth authorization")]
    AuthorityMismatch,
    #[error("the exact registered Schwab publication generation is unavailable")]
    GenerationUnavailable,
    #[error("Schwab quote source evidence is incomplete or inconsistent")]
    SourceEvidence,
    #[error("Schwab quote runtime requires current accepted canonical provider identity")]
    CanonicalIdentity,
    #[error("Schwab quote runtime requires fresh canonical reference identity resolution")]
    IdentityResolutionRequired,
    #[error("Schwab quote code-owned resource controls are invalid")]
    InvalidControls,
    #[error("the trusted local clock is unavailable")]
    Clock,
    #[error(transparent)]
    Activation(#[from] SchwabMarketDataActivationError),
    #[error(transparent)]
    OAuth(#[from] crate::provider_onboarding::SchwabOAuthRuntimeError),
    #[error(transparent)]
    Research(#[from] ResearchIngestCompositionError),
    #[error(transparent)]
    Publication(#[from] SchwabMarketPublicationError),
    #[error(transparent)]
    Runtime(#[from] SchwabRestQuoteRuntimeError),
}

/// Schwab read-only market-data account activation failure.
#[derive(Debug, thiserror::Error)]
pub enum SchwabMarketDataActivationError {
    #[error("Schwab market-data activation was cancelled")]
    Cancelled,
    #[error("Schwab OAuth, application, or onboarding authority does not match")]
    AuthorityMismatch,
    #[error(transparent)]
    Account(#[from] ProviderAccountActivationError),
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
    #[error(transparent)]
    OAuth(#[from] crate::provider_onboarding::SchwabOAuthRuntimeError),
}

/// Original canonical and listing revisions retained through the same catalog transaction.
#[derive(Debug)]
pub(crate) struct SchwabQuoteReferencePrecommit {
    canonical: market_squawk_data::MarketDataInstrumentReadCapability,
    records: Vec<market_squawk_data::MarketDataInstrumentRecord>,
    listing: Option<(
        market_squawk_data::ListingReferenceReadCapability,
        market_squawk_data::ListingReferenceGenerationReceipt,
    )>,
    deadline: std::time::Instant,
    cancellation: tokio_util::sync::CancellationToken,
}
impl SchwabQuoteReferencePrecommit {
    pub(crate) fn new(
        canonical: market_squawk_data::MarketDataInstrumentReadCapability,
        records: Vec<market_squawk_data::MarketDataInstrumentRecord>,
        listing: Option<(
            market_squawk_data::ListingReferenceReadCapability,
            market_squawk_data::ListingReferenceGenerationReceipt,
        )>,
        deadline: std::time::Instant,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            canonical,
            records,
            listing,
            deadline,
            cancellation,
        }
    }
    pub(crate) fn validate_catalog(
        &self,
        catalog: &market_squawk_data::CatalogAuthority,
    ) -> Result<(), market_squawk_data::IngestError> {
        if self.records.is_empty() || self.records.len() > 50 {
            return Err(market_squawk_data::IngestError::PublicationAuthorityRevoked);
        }
        for record in &self.records {
            self.canonical
                .require_current_in_catalog(catalog, record, self.deadline, &self.cancellation)
                .map_err(|_| market_squawk_data::IngestError::PublicationAuthorityRevoked)?;
        }
        if let Some((reader, expected)) = &self.listing {
            reader
                .require_current_in_catalog(catalog, expected, self.deadline, &self.cancellation)
                .map_err(|_| market_squawk_data::IngestError::PublicationAuthorityRevoked)?;
        }
        Ok(())
    }
}
