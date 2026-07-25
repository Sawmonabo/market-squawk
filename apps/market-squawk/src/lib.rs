//! Local-first Market Squawk application composition.
//!
//! Production live batches enter only [`live_runtime::LiveRuntimeComposition`]. The legacy local
//! event model is explicitly diagnostic and remains isolated from current execution authority.

pub mod application;
mod artifact_repository;
pub mod backtest_service;
pub mod backtest_strategy;
pub mod bot;
pub mod cli;
pub mod diagnostic_engine;
mod domain;
pub mod features;
pub mod live_runtime;
mod live_source;
pub mod local_product;
pub mod mcp;
pub mod order_book;
pub mod paper_bot;
pub mod portfolio_application;
pub mod provider_activation;
pub mod provider_onboarding;
mod provider_rate;
pub mod quality;
pub mod replay;
pub mod research_service;
pub mod risk;
pub mod source;
pub mod source_supervisor;

/// Platform journal compatibility facade retained for existing application imports.
pub mod journal {
    pub use market_squawk_platform::{JournalError, JournalReader, JournalWriter};
}

pub use backtest_service::{
    BacktestExperimentPlan, PinnedBacktestInput, ProductionBacktestService,
    ProductionBacktestServiceError,
};
pub use diagnostic_engine::{
    DiagnosticEngine, DiagnosticEngineSnapshot, DiagnosticProductSnapshot, SharedDiagnosticEngine,
};
pub use domain::{
    BookChange as DiagnosticBookChange, MarketEvent as DiagnosticMarketEvent,
    PriceLevel as DiagnosticPriceLevel, RawEnvelope as DiagnosticRawEnvelope,
    Side as DiagnosticSide,
};
pub use live_runtime::{LiveRuntimeComposition, LiveRuntimeCompositionError};
pub use live_source::{
    ProductionCoinbaseProfileError, ProductionLiveSourceComposition,
    ProductionLiveSourceCompositionError, ProductionLiveSourceRuntime,
    ProductionLiveSourceRuntimeError, ProductionSourceProvider, ProductionSupervisorError,
};
pub use local_product::{LocalProduct, LocalProductError};
pub use market_squawk_platform::{
    AppConfig, JournalFileFormat, JournalSelectionError, LocalPaths as AppPaths,
};
pub use paper_bot::{
    ProductionAuditBarrierError, ProductionAuditError, ProductionAuditEvidence,
    ProductionAuditShutdown, ProductionAuditShutdownStatus, ProductionPaperBotComposition,
    ProductionPaperBotCompositionError, ProductionPaperBotExecutionConfig,
    ProductionPaperBotRollback, ProductionPaperBotRoute, ProductionPaperBotRuntime,
    ProductionPaperBotShutdown, ProductionPaperBotStartError, ProductionPaperCheckpointError,
    ProductionPaperCheckpointEvidence,
};
pub use portfolio_application::{
    PortfolioApplicationLimitInput, PortfolioApplicationLimits, PortfolioApplicationService,
    PortfolioApplicationServiceError, PortfolioFairValueReadCapability,
};
pub use provider_activation::{
    ActivatedResearchProvider, BlsAdapterActivation, FredAdapterActivation, LiveProviderActivation,
    LocalFileAdapterActivation, PortfolioAdapterActivation, ProviderActivationOutcome,
    ProviderAdapterActivation, ProviderAdapterActivationError, ProviderAdapterActivationRequest,
    SecAdapterActivation, TreasuryAdapterActivation,
};
pub use provider_onboarding::{
    OnboardingNextAction, OnboardingSessionView, ProviderActivationLease, ProviderOnboardingError,
    ProviderOnboardingPortal, ProviderOnboardingService, ProviderPortalActivationAuthority,
    ProviderPortalActivationError, ProviderPortalActivationRequest, ProviderPortalActivationView,
    ProviderPortalConfig, ProviderPortalError, ProviderProfileRegistration,
    ProviderProfileRegistrationOutcome, ProviderProfileView, StartOnboardingRequest,
};
pub use research_service::{ResearchIngestRequest, ResearchService, ResearchServiceError};
