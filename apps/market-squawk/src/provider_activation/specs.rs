//! Typed, capability-bearing provider activation inputs.

use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};

use market_squawk_adapter_bls::BlsSeriesMetadata;
use market_squawk_adapter_coinbase::{
    CoinbaseConfigError, CoinbaseDirectLimits, CoinbaseProductMapping,
};
use market_squawk_adapter_files::ExtractionLimits;
use market_squawk_adapter_fred::FredRightsPolicy;
use market_squawk_adapter_portfolio::PortfolioImportLimits;
use market_squawk_adapter_sec::{RawEvidenceStore, SecParserLimits, SecRepresentationRegistry};
use market_squawk_adapter_treasury::TreasurySourceConfig;
use market_squawk_domain::{ProviderIdentityRegistry, ProviderProduct};
use market_squawk_live::LiveRouteConfig;
use market_squawk_platform::{
    BoundedInput, LocalAuthorityStateStore, SecretReference, UserAuthorizedInputRoot,
    UserOwnedInputEvidence,
};
use market_squawk_sources::{FreshnessPolicy, SourceMetadata};

use crate::application::ResearchIngestCompositionError;

/// Coinbase Direct account subscription ceiling for the exact `full` product/channel pair.
pub const COINBASE_DIRECT_MAXIMUM_SUBSCRIPTIONS: usize = 10;
const COINBASE_EXCHANGE_VENUE: &str = "coinbase-exchange";

/// Closed configuration for one Coinbase Direct product and its sole live route.
#[derive(Clone, Debug)]
pub struct CoinbaseDirectProductActivation {
    pub(super) mapping: CoinbaseProductMapping,
    pub(super) route: LiveRouteConfig,
    pub(super) freshness: FreshnessPolicy,
    pub(super) limits: CoinbaseDirectLimits,
}

impl CoinbaseDirectProductActivation {
    /// Binds one provider product to one stable instrument route and all generation limits.
    ///
    /// # Errors
    ///
    /// Rejects a product outside the pinned Coinbase Exchange grammar or a route whose current
    /// Coinbase venue symbol does not name that exact product.
    pub fn try_new(
        product: ProviderProduct,
        route: LiveRouteConfig,
        freshness: FreshnessPolicy,
        limits: CoinbaseDirectLimits,
    ) -> Result<Self, CoinbaseDirectActivationSpecError> {
        let mapping = CoinbaseProductMapping::try_new(product, route.route().instrument())?;
        let venue_mapping = route
            .definition()
            .venue_mappings()
            .iter()
            .find(|candidate| candidate.venue_id() == route.route().venue())
            .ok_or(CoinbaseDirectActivationSpecError::RouteMismatch)?;
        if route.route().venue().as_str() != COINBASE_EXCHANGE_VENUE
            || venue_mapping.venue_symbol().as_str()
                != mapping.product().as_source_identifier().as_str()
        {
            return Err(CoinbaseDirectActivationSpecError::RouteMismatch);
        }
        Ok(Self {
            mapping,
            route,
            freshness,
            limits,
        })
    }

    /// Returns the exact provider product reserved by this connection.
    pub const fn product(&self) -> &ProviderProduct {
        self.mapping.product()
    }

    /// Returns the sole internal live route for the product.
    pub const fn route(&self) -> &LiveRouteConfig {
        &self.route
    }

    /// Returns the sealed provider-product to internal-instrument mapping.
    pub const fn mapping(&self) -> &CoinbaseProductMapping {
        &self.mapping
    }

    /// Returns the source-data freshness contract for this exact product.
    pub const fn freshness(&self) -> &FreshnessPolicy {
        &self.freshness
    }

    /// Returns every transport, snapshot, replay, book, and publication bound.
    pub const fn limits(&self) -> CoinbaseDirectLimits {
        self.limits
    }
}

/// Complete pre-network account-level admission request for Coinbase Direct.
#[derive(Debug)]
pub struct CoinbaseDirectAdapterActivation {
    pub(super) products: Vec<CoinbaseDirectProductActivation>,
    pub(super) maximum_runtime_bytes: NonZeroU64,
    pub(super) capture_queue_records_per_product: NonZeroUsize,
    pub(super) capture_queue_bytes_per_product: NonZeroUsize,
    pub(super) supervisor_queue_records: NonZeroUsize,
    pub(super) supervisor_queue_bytes: NonZeroUsize,
}

impl CoinbaseDirectAdapterActivation {
    /// Validates the complete unique product/route set before any account authority is acquired.
    ///
    /// # Errors
    ///
    /// Rejects an empty, duplicate, wrong-venue, or over-cap product set.
    pub fn try_new(
        mut products: Vec<CoinbaseDirectProductActivation>,
        maximum_runtime_bytes: NonZeroU64,
        capture_queue_records_per_product: NonZeroUsize,
        capture_queue_bytes_per_product: NonZeroUsize,
        supervisor_queue_records: NonZeroUsize,
        supervisor_queue_bytes: NonZeroUsize,
    ) -> Result<Self, CoinbaseDirectActivationSpecError> {
        if products.is_empty() || products.len() > COINBASE_DIRECT_MAXIMUM_SUBSCRIPTIONS {
            return Err(CoinbaseDirectActivationSpecError::SubscriptionCardinality);
        }
        products.sort_by(|left, right| {
            left.product()
                .as_source_identifier()
                .as_str()
                .cmp(right.product().as_source_identifier().as_str())
        });
        for (index, product) in products.iter().enumerate() {
            if product.route.route().venue().as_str() != COINBASE_EXCHANGE_VENUE {
                return Err(CoinbaseDirectActivationSpecError::RouteMismatch);
            }
            if products[index.saturating_add(1)..].iter().any(|other| {
                other.product() == product.product() || other.route.route() == product.route.route()
            }) {
                return Err(CoinbaseDirectActivationSpecError::DuplicateSubscription);
            }
        }
        Ok(Self {
            products,
            maximum_runtime_bytes,
            capture_queue_records_per_product,
            capture_queue_bytes_per_product,
            supervisor_queue_records,
            supervisor_queue_bytes,
        })
    }

    /// Returns the complete product set that will be atomically reserved.
    pub fn products(&self) -> &[CoinbaseDirectProductActivation] {
        &self.products
    }
}

/// Invalid Coinbase Direct account activation topology.
#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum CoinbaseDirectActivationSpecError {
    /// Coinbase product syntax or mapping construction violated the pinned adapter contract.
    #[error(transparent)]
    Coinbase(#[from] CoinbaseConfigError),
    /// The complete account set is empty or exceeds the ten product/channel subscriptions.
    #[error("Coinbase Direct subscription cardinality is invalid")]
    SubscriptionCardinality,
    /// Two configured connections claim the same product or live route.
    #[error("Coinbase Direct contains a duplicate product or route")]
    DuplicateSubscription,
    /// A Direct product is not bound to the Coinbase Exchange venue.
    #[error("Coinbase Direct route does not use the Coinbase Exchange venue")]
    RouteMismatch,
    /// Checked product, queue, capture, and publication memory exceeded the configured ceiling.
    #[error("Coinbase Direct runtime memory admission failed")]
    MemoryAdmission,
}

/// SEC adapter construction inputs whose filesystem authority is already capability-confined.
pub struct SecAdapterActivation {
    pub(super) metadata: SourceMetadata,
    pub(super) raw_store: RawEvidenceStore,
    pub(super) representations: SecRepresentationRegistry,
    pub(super) identities: ProviderIdentityRegistry,
    pub(super) parser_limits: SecParserLimits,
}

impl SecAdapterActivation {
    /// Retains exact metadata, durable store capabilities, parser ceilings, and rights evidence.
    #[must_use]
    pub fn new(
        metadata: SourceMetadata,
        raw_store: RawEvidenceStore,
        representations: SecRepresentationRegistry,
        identities: ProviderIdentityRegistry,
        parser_limits: SecParserLimits,
    ) -> Self {
        Self {
            metadata,
            raw_store,
            representations,
            identities,
            parser_limits,
        }
    }
}

impl fmt::Debug for SecAdapterActivation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecAdapterActivation")
            .field("source_id", self.metadata.source_id())
            .field("metadata_revision", self.metadata.revision())
            .field("stores", &"[CAPABILITY-CONFINED]")
            .finish()
    }
}

/// BLS adapter construction inputs excluding credential material.
#[derive(Debug)]
pub struct BlsAdapterActivation {
    pub(super) metadata: SourceMetadata,
    pub(super) series: Vec<BlsSeriesMetadata>,
    pub(super) start_year: u16,
    pub(super) end_year: u16,
}

impl BlsAdapterActivation {
    /// Retains the exact request universe, years, metadata, and persistence evidence.
    #[must_use]
    pub fn new(
        metadata: SourceMetadata,
        series: Vec<BlsSeriesMetadata>,
        start_year: u16,
        end_year: u16,
    ) -> Self {
        Self {
            metadata,
            series,
            start_year,
            end_year,
        }
    }
}

/// Treasury adapter construction inputs for one exact Fiscal Data or XML family.
#[derive(Debug)]
pub struct TreasuryAdapterActivation {
    pub(super) metadata: SourceMetadata,
    pub(super) config: TreasurySourceConfig,
}

impl TreasuryAdapterActivation {
    /// Retains exact provider configuration, metadata, and persistence-rights evidence.
    #[must_use]
    pub fn new(metadata: SourceMetadata, config: TreasurySourceConfig) -> Self {
        Self { metadata, config }
    }
}

/// FRED construction inputs accepted only after an exact scope-specific active lease exists.
#[derive(Debug)]
pub struct FredAdapterActivation {
    pub(super) metadata: SourceMetadata,
    pub(super) policy: FredRightsPolicy,
}

impl FredAdapterActivation {
    /// Retains the exact per-series policy, metadata, and independent persistence evidence.
    #[must_use]
    pub fn new(metadata: SourceMetadata, policy: FredRightsPolicy) -> Self {
        Self { metadata, policy }
    }
}

/// Explicit user-root and manifest authority for one local-file research source.
pub struct LocalFileAdapterActivation {
    pub(super) metadata: SourceMetadata,
    pub(super) root: UserAuthorizedInputRoot,
    pub(super) representation_state_root: PathBuf,
    pub(super) manifest: BoundedInput,
    pub(super) limits: ExtractionLimits,
    pub(super) ownership: UserOwnedInputEvidence,
}

impl LocalFileAdapterActivation {
    /// Retains a pre-opened user root, two-pass manifest, disjoint state root, and ownership proof.
    #[must_use]
    pub fn new(
        metadata: SourceMetadata,
        root: UserAuthorizedInputRoot,
        representation_state_root: impl AsRef<Path>,
        manifest: BoundedInput,
        limits: ExtractionLimits,
        ownership: UserOwnedInputEvidence,
    ) -> Self {
        Self {
            metadata,
            root,
            representation_state_root: representation_state_root.as_ref().to_path_buf(),
            manifest,
            limits,
            ownership,
        }
    }
}

impl fmt::Debug for LocalFileAdapterActivation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalFileAdapterActivation")
            .field("source_id", self.metadata.source_id())
            .field("metadata_revision", self.metadata.revision())
            .field("root", &"[USER-AUTHORIZED]")
            .field("manifest", &self.manifest)
            .field("representation_state_root", &"[CONTROLLED]")
            .finish()
    }
}

/// Explicit user-root, manifest, and durable archive authority for portfolio imports.
pub struct PortfolioAdapterActivation {
    pub(super) metadata: SourceMetadata,
    pub(super) root: UserAuthorizedInputRoot,
    pub(super) manifest_reference: PathBuf,
    pub(super) manifest: BoundedInput,
    pub(super) archive: LocalAuthorityStateStore,
    pub(super) credential: Option<SecretReference>,
    pub(super) limits: PortfolioImportLimits,
    pub(super) ownership: UserOwnedInputEvidence,
}

impl PortfolioAdapterActivation {
    /// Retains one exact manifest beneath a user root and one durable raw-import archive.
    #[allow(
        clippy::too_many_arguments,
        reason = "each argument is distinct input, archive, provenance, or capacity authority"
    )]
    #[must_use]
    pub fn new(
        metadata: SourceMetadata,
        root: UserAuthorizedInputRoot,
        manifest_reference: impl AsRef<Path>,
        manifest: BoundedInput,
        archive: LocalAuthorityStateStore,
        credential: Option<SecretReference>,
        limits: PortfolioImportLimits,
        ownership: UserOwnedInputEvidence,
    ) -> Self {
        Self {
            metadata,
            root,
            manifest_reference: manifest_reference.as_ref().to_path_buf(),
            manifest,
            archive,
            credential,
            limits,
            ownership,
        }
    }
}

impl fmt::Debug for PortfolioAdapterActivation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PortfolioAdapterActivation")
            .field("source_id", self.metadata.source_id())
            .field("metadata_revision", self.metadata.revision())
            .field("root", &"[USER-AUTHORIZED]")
            .field("manifest_reference", &self.manifest_reference)
            .field("manifest", &self.manifest)
            .field("archive", &"[DURABLE LOCAL AUTHORITY]")
            .field(
                "credential",
                &self.credential.as_ref().map(|_| "[REFERENCE]"),
            )
            .field("limits", &self.limits)
            .finish()
    }
}

/// Activation input for one closed provider family.
#[derive(Debug)]
pub enum ProviderAdapterActivationRequest {
    /// Coinbase or Kraken live routes, selected by the lease surface.
    Live(Vec<market_squawk_live::LiveRouteConfig>),
    /// Authenticated Coinbase Direct account runtime with one bounded connection per product.
    CoinbaseDirect(CoinbaseDirectAdapterActivation),
    /// SEC EDGAR research extraction.
    Sec(SecAdapterActivation),
    /// BLS public-v1 or registered-v2 research extraction.
    Bls(BlsAdapterActivation),
    /// Treasury Fiscal Data or daily-rate XML extraction.
    Treasury(TreasuryAdapterActivation),
    /// FRED/ALFRED extraction under exact scope-specific rights.
    Fred(FredAdapterActivation),
    /// User-owned local file extraction.
    LocalFiles(LocalFileAdapterActivation),
    /// User-owned portfolio holdings and transactions extraction.
    Portfolio(PortfolioAdapterActivation),
}

/// Provider activation or adapter construction failure.
#[derive(Debug, thiserror::Error)]
pub enum ProviderAdapterActivationError {
    /// The request kind does not match the exact active onboarding surface.
    #[error("provider activation request does not match the active surface")]
    SurfaceMismatch,
    /// Source metadata and retained rights name different source authority.
    #[error("provider activation source binding does not match")]
    SourceBinding,
    /// Reviewed rights evidence is structurally invalid.
    #[error("provider activation rights evidence is invalid")]
    InvalidRights,
    /// The caller cancelled before a synchronous construction boundary.
    #[error("provider activation was cancelled")]
    Cancelled,
    /// A platform-managed credential may be read only from an explicit foreground request.
    #[error("provider activation requires explicit foreground credential resume")]
    ExplicitResumeRequired,
    /// Provider onboarding has not produced an active immutable lease.
    #[error(transparent)]
    Onboarding(#[from] crate::ProviderOnboardingError),
    /// The shared research coordinator rejected source admission.
    #[error(transparent)]
    Research(#[from] ResearchIngestCompositionError),
    /// SEC construction rejected metadata, contact, storage, or protocol authority.
    #[error(transparent)]
    Sec(#[from] market_squawk_adapter_sec::SecClientError),
    /// BLS construction rejected metadata, authorization, or request scope.
    #[error(transparent)]
    Bls(#[from] market_squawk_adapter_bls::BlsSourceError),
    /// Treasury construction rejected metadata or provider profile.
    #[error(transparent)]
    Treasury(#[from] market_squawk_adapter_treasury::TreasurySourceError),
    /// FRED construction rejected exact key, rights, or metadata authority.
    #[error(transparent)]
    Fred(#[from] market_squawk_adapter_fred::FredSourceError),
    /// Local-file construction rejected root, manifest, metadata, or storage authority.
    #[error(transparent)]
    Files(#[from] market_squawk_adapter_files::FileAdapterError),
    /// Portfolio construction rejected root, manifest, metadata, archive, or input authority.
    #[error(transparent)]
    Portfolio(#[from] market_squawk_adapter_portfolio::PortfolioManifestSourceError),
    /// Process TLS installation did not produce project-owned authority.
    #[error(transparent)]
    Tls(#[from] market_squawk_sources::TlsProviderError),
    /// Live source configuration or route binding was invalid.
    #[error(transparent)]
    Live(#[from] crate::ProductionLiveSourceCompositionError),
    /// Coinbase Direct account/product topology is invalid.
    #[error(transparent)]
    CoinbaseDirectSpec(#[from] CoinbaseDirectActivationSpecError),
    /// Coinbase Direct account-scoped durable lifetime ownership is unavailable.
    #[error(transparent)]
    CoinbaseDirectAuthority(#[from] market_squawk_platform::LocalAuthorityStateStoreError),
    /// Coinbase Direct control-state paths are unsafe or unavailable.
    #[error(transparent)]
    CoinbaseDirectPath(#[from] market_squawk_platform::PathError),
    /// Durable provider authorization-subject admission was unavailable or inconsistent.
    #[error(transparent)]
    ProviderRate(#[from] market_squawk_sources::ProviderRateStoreError),
    /// Process-local bounded extraction authority was unavailable or inconsistent.
    #[error(transparent)]
    Registry(#[from] market_squawk_sources::RegistryError),
    /// A bounded extraction request violated its closed contract.
    #[error(transparent)]
    ExtractionContract(#[from] market_squawk_sources::ExtractionError),
    /// The provider rejected or could not complete bounded extraction.
    #[error(transparent)]
    ExtractionSource(#[from] market_squawk_sources::ExtractionSourceError),
}
