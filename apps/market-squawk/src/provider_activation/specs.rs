//! Typed, capability-bearing provider activation inputs.

use std::fmt;
use std::path::{Path, PathBuf};

use market_squawk_adapter_bls::BlsSeriesMetadata;
use market_squawk_adapter_files::ExtractionLimits;
use market_squawk_adapter_fred::FredRightsPolicy;
use market_squawk_adapter_portfolio::PortfolioImportLimits;
use market_squawk_adapter_sec::{RawEvidenceStore, SecParserLimits, SecRepresentationRegistry};
use market_squawk_adapter_treasury::TreasurySourceConfig;
use market_squawk_domain::ProviderIdentityRegistry;
use market_squawk_platform::{
    BoundedInput, LocalAuthorityStateStore, SecretReference, UserAuthorizedInputRoot,
    UserOwnedInputEvidence,
};
use market_squawk_sources::SourceMetadata;

use crate::application::ResearchIngestCompositionError;

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
}
