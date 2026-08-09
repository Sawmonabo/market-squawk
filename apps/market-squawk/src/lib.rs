#![cfg_attr(all(test, target_os = "macos"), allow(linker_messages))]

//! Self-hosted Market Squawk application composition.
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
pub mod doctor;
mod domain;
pub mod features;
pub mod jobs;
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
pub mod release;
pub mod replay;
pub mod research_service;
pub mod risk;
pub mod service;
pub mod source;
pub mod source_supervisor;
pub mod termination;

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
    CoinbaseDirectLiveRuntime, CoinbaseDirectOutputFailure, CoinbaseDirectProductRuntimeError,
    CoinbaseDirectSupervisorError, ProductionCoinbaseProfileError, ProductionLiveSourceComposition,
    ProductionLiveSourceCompositionError, ProductionLiveSourceRuntime,
    ProductionLiveSourceRuntimeError, ProductionSourceProvider, ProductionSupervisorError,
};
pub use local_product::{
    LocalMcpAvailabilityError, LocalProduct, LocalProductError, LocalServiceAvailabilityError,
    verified_installed_cli_program, verified_installed_service_program,
};
#[cfg(debug_assertions)]
pub use local_product::{
    verified_development_mcp_relay_program, verified_development_service_program,
};
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
    ActivatedResearchProvider, BlsAdapterActivation, COINBASE_DIRECT_MAXIMUM_SUBSCRIPTIONS,
    CoinbaseDirectAccountActivation, CoinbaseDirectActivationSpecError,
    CoinbaseDirectAdapterActivation, CoinbaseDirectProductActivation,
    CoinbaseDirectRuntimeAdmission, FredAdapterActivation, LiveProviderActivation,
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
