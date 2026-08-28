//! Product composition for the single durable provider-rate authority.

mod tiingo;

use std::path::Path;
use std::sync::Arc;

use market_squawk_data::SqliteProviderRateStore;
use market_squawk_sources::{ProviderRateAuthority, ProviderRateStoreError};

const PROVIDER_RATE_DATABASE: &str = "provider-rate-authority.sqlite3";

pub(crate) fn open_provider_rate_authority(
    control_root: &Path,
) -> Result<ProviderRateAuthority, ProviderRateStoreError> {
    provider_rate_authority_from_store(open_provider_rate_store(control_root)?)
}

/// Opens the single owner-held store so backup composition can retain its logical checkpoint
/// without attempting to take a second provider-rate owner lease.
pub(crate) fn open_provider_rate_store(
    control_root: &Path,
) -> Result<Arc<SqliteProviderRateStore>, ProviderRateStoreError> {
    Ok(Arc::new(SqliteProviderRateStore::try_open(
        control_root.join(PROVIDER_RATE_DATABASE),
    )?))
}

/// Starts the runtime capability from the already owner-held durable store.
pub(crate) fn provider_rate_authority_from_store(
    store: Arc<SqliteProviderRateStore>,
) -> Result<ProviderRateAuthority, ProviderRateStoreError> {
    ProviderRateAuthority::try_new(store)
}
