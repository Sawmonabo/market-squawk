//! Product composition for the single durable provider-rate authority.

use std::path::Path;
use std::sync::Arc;

use market_squawk_data::SqliteProviderRateStore;
use market_squawk_sources::{ProviderRateAuthority, ProviderRateStoreError};

const PROVIDER_RATE_DATABASE: &str = "provider-rate-authority.sqlite3";

pub(crate) fn open_provider_rate_authority(
    control_root: &Path,
) -> Result<ProviderRateAuthority, ProviderRateStoreError> {
    let store = Arc::new(SqliteProviderRateStore::try_open(
        control_root.join(PROVIDER_RATE_DATABASE),
    )?);
    ProviderRateAuthority::try_new(store)
}
