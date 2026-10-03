//! Installed account composition for demand-owned indicative option chains.
use super::{AlpacaBasicAccountActivation, MarketDataInstrumentBinding, ProviderAdapterActivation};
use crate::application::{AlpacaOptionChainRuntime, OptionChainDemandError};
use market_squawk_adapter_alpaca::AlpacaOptionChainConfig;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

impl ProviderAdapterActivation {
    pub(crate) fn prepare_alpaca_option_chain_child(
        &self,
        activation: &AlpacaBasicAccountActivation,
        config: AlpacaOptionChainConfig,
        underlyings: Vec<MarketDataInstrumentBinding>,
        cancellation: CancellationToken,
    ) -> Result<AlpacaOptionChainRuntime, OptionChainDemandError> {
        if cancellation.is_cancelled()
            || underlyings.is_empty()
            || !activation
                .account_binding()
                .validates_metadata(config.metadata())
        {
            return Err(OptionChainDemandError::Authority);
        }
        let bounds = config.request_bounds();
        let (generation, rights) =
            super::public_live_runtime_generation(activation.lease(), config.metadata())
                .map_err(|_| OptionChainDemandError::Authority)?;
        let authority = activation
            .bind_option_chain_runtime(config, cancellation.clone())
            .map_err(|_| OptionChainDemandError::Authority)?;
        self.research_mutation
            .register_provider_publication_generation(generation.clone(), rights)
            .map_err(|_| OptionChainDemandError::Authority)?;
        let (research, registration) = self
            .research
            .bind_alpaca_option_chain_runtime(&generation, cancellation.clone())
            .map_err(|_| OptionChainDemandError::Authority)?;
        AlpacaOptionChainRuntime::start(
            authority,
            generation,
            Arc::clone(&self.research),
            research,
            bounds,
            activation.lease().rights_decision_digest(),
            activation.lease().capability_digest(),
            underlyings,
            cancellation,
            registration,
        )
    }
}
