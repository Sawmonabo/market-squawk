//! Validated platform-to-adapter instrument composition.

use market_squawk_adapter_coinbase::{CoinbaseConfigError, CoinbaseProductMapping};
use market_squawk_domain::{ProviderProduct, SourceIdentifier};
use market_squawk_platform::CoinbaseSourceConfig;
use thiserror::Error;

#[derive(Debug)]
pub(super) struct ProductionInstrumentSet {
    adapter_mappings: Box<[CoinbaseProductMapping]>,
}

impl ProductionInstrumentSet {
    pub(super) fn adapter_mappings(&self) -> &[CoinbaseProductMapping] {
        &self.adapter_mappings
    }
}

impl TryFrom<&CoinbaseSourceConfig> for ProductionInstrumentSet {
    type Error = ProductionInstrumentError;

    fn try_from(config: &CoinbaseSourceConfig) -> Result<Self, Self::Error> {
        let mut adapter_mappings = Vec::new();
        adapter_mappings
            .try_reserve_exact(config.instruments().len())
            .map_err(|_error| ProductionInstrumentError::AllocationFailed)?;
        for mapping in config.instruments() {
            let definition = mapping.definition().clone();
            let product = ProviderProduct::new(SourceIdentifier::try_from(mapping.product())?);
            adapter_mappings.push(CoinbaseProductMapping::try_new(
                product,
                definition.instrument_id(),
            )?);
        }
        Ok(Self {
            adapter_mappings: adapter_mappings.into_boxed_slice(),
        })
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ProductionInstrumentError {
    #[error("production instrument mapping allocation failed")]
    AllocationFailed,
    #[error("production instrument identity is invalid")]
    Identity(#[from] market_squawk_domain::IdentityError),
    #[error("Coinbase adapter mapping rejected validated configuration")]
    Adapter(#[from] CoinbaseConfigError),
}
