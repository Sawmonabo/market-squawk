//! Selected public Coinbase configuration shared by integration tests that need live authority.

#[path = "catalog.rs"]
mod catalog;

use market_squawk_adapter_coinbase::{
    CoinbaseChannel, CoinbaseExchangeConfig, CoinbaseProductMapping,
};
use market_squawk_domain::ProviderProduct;

use crate::common::{TestResult, config, config_with_channels_and_mapping, identifier};
use catalog::CatalogFixture;

pub(crate) struct SelectedFixture {
    pub(crate) config: CoinbaseExchangeConfig,
    pub(crate) catalog: CatalogFixture,
}

pub(crate) fn selected_fixture() -> TestResult<SelectedFixture> {
    let base = config()?;
    let instrument = base
        .mappings()
        .first()
        .ok_or("Coinbase product mapping missing")?
        .instrument();
    let catalog = CatalogFixture::new(instrument)?;
    let config = config_with_channels_and_mapping(
        vec![
            CoinbaseChannel::Level2,
            CoinbaseChannel::MarketTrades,
            CoinbaseChannel::Heartbeats,
        ],
        CoinbaseProductMapping::try_new_selected_public(
            ProviderProduct::new(identifier("BTC-USD")?),
            instrument,
            catalog.selected.clone(),
        )?,
    )?;
    Ok(SelectedFixture { config, catalog })
}
