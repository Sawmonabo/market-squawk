//! Product composition for the single durable provider-rate authority.

mod schwab_streamer;
mod tiingo;
pub(crate) use schwab_streamer::GovernedSchwabStreamer;

pub(crate) use tiingo::DurableTiingoProviderAuthority;

use std::path::Path;
use std::sync::Arc;

use market_squawk_data::SqliteProviderRateStore;
use market_squawk_sources::{
    ProviderRateAuthority, ProviderRateDeclaration, ProviderRateStoreError, SEC_EDGAR_AUTHORITY,
    SEC_EDGAR_PROFILE_ID, built_in_provider_profiles,
};

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
    let declaration = ProviderRateDeclaration::try_for_endpoint(
        SEC_EDGAR_AUTHORITY
            .budget_policy()
            .map_err(|_| ProviderRateStoreError::Corrupt)?,
        &SEC_EDGAR_AUTHORITY
            .endpoint_policy()
            .map_err(|_| ProviderRateStoreError::Corrupt)?,
    )
    .map_err(|_| ProviderRateStoreError::Corrupt)?;
    // Onboarding and source acquisition share the SEC group but declare different endpoint
    // sets. Configure both current producers atomically, without erasing either association.
    let profiles = built_in_provider_profiles().map_err(|_| ProviderRateStoreError::Corrupt)?;
    let profile = profiles
        .get(SEC_EDGAR_PROFILE_ID)
        .ok_or(ProviderRateStoreError::Corrupt)?;
    let probe = ProviderRateDeclaration::try_for_endpoint(
        profile
            .rate_policy()
            .enforcement_policy()
            .cloned()
            .ok_or(ProviderRateStoreError::Corrupt)?,
        profile
            .probe()
            .endpoint_policy()
            .ok_or(ProviderRateStoreError::Corrupt)?,
    )
    .map_err(|_| ProviderRateStoreError::Corrupt)?;
    ProviderRateAuthority::try_new_with_request_configuration(store, &[declaration, probe])
}
