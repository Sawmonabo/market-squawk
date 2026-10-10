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

#[cfg(test)]
mod tests {
    use super::*;
    use market_squawk_domain::Timestamp;
    use market_squawk_platform::{
        InstalledServiceInstanceGuard, LocalAuthorityStateStore, LocalPaths,
    };
    use market_squawk_sources::{
        AuthoritativeSourceRegistry, AuthorizationSubjectResolver, FASB_XBRL_TAXONOMY_AUTHORITY,
        RegistryError,
    };

    // Recovery must use the same shared quota store as the live producer: these registries
    // deliberately retain no duplicate local quota checkpoint.
    #[test]
    fn live_replacement_reopens_shared_quota_without_local_checkpoints()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let installation = LocalPaths::prepare(temporary.path().join("installation"))?;
        let workspace = LocalPaths::prepare(temporary.path().join("workspace"))?;
        let selected = InstalledServiceInstanceGuard::try_acquire(installation.control_root()?)?
            .bind_selected_workspace(workspace.clone())?;
        let rate = open_provider_rate_authority(workspace.control_root()?.root())?;
        let resolver: Arc<dyn AuthorizationSubjectResolver> = Arc::new(rate.clone());
        let key = "shared-live-recovery";
        let authority_path = workspace.root().join("authority").join(key);
        let metadata = FASB_XBRL_TAXONOMY_AUTHORITY.dependency_source_metadata()?;
        let mut registry = AuthoritativeSourceRegistry::try_new_durable_with_authorization_subject_resolver_and_provider_rate(
            LocalAuthorityStateStore::try_open(&authority_path)?, Arc::clone(&resolver), rate.clone(),
        )?;
        let at = Timestamp::from_unix_nanos(
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .ok_or("clock overflow")?,
        );
        let registered = registry.register_or_resume_exact(metadata.clone(), at)?;
        let expected = registry.export_authority_state()?;
        drop(registered);
        registry.shutdown()?;

        {
            let store = LocalAuthorityStateStore::try_open(&authority_path)?;
            let payload = store.load()?.ok_or("source checkpoint absent")?;
            let envelope: serde_json::Value = serde_json::from_slice(&payload)?;
            assert!(
                envelope["budgets"]
                    .as_array()
                    .ok_or("budget list absent")?
                    .is_empty()
            );
        }
        // This reproduces the former installed-startup failure instead of relaxing local-only
        // validation to accept a quota association it cannot validate.
        assert!(matches!(
            AuthoritativeSourceRegistry::try_new_durable_with_authorization_subject_resolver(
                LocalAuthorityStateStore::try_open(&authority_path)?,
                Arc::clone(&resolver),
            ),
            Err(RegistryError::InvalidAuthorityState)
        ));
        let database = rusqlite::Connection::open_with_flags(
            workspace
                .control_root()?
                .root()
                .join(PROVIDER_RATE_DATABASE),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let quota_state = || -> Result<Vec<Vec<u8>>, rusqlite::Error> {
            database
                .prepare("SELECT state_json FROM provider_rate_groups ORDER BY group_id")?
                .query_map([], |row| row.get(0))?
                .collect()
        };
        let before = quota_state()?;
        assert!(!before.is_empty());
        for _ in 0..2 {
            AuthoritativeSourceRegistry::reconcile_live_authority_for_exclusive_installed_service_replacement(
                &selected, key, Arc::clone(&resolver), rate.clone(),
            )?;
        }
        assert_eq!(
            quota_state()?,
            before,
            "recovery changed aggregate quota state"
        );
        let mut reopened = AuthoritativeSourceRegistry::try_new_durable_with_authorization_subject_resolver_and_provider_rate(
            LocalAuthorityStateStore::try_open(&authority_path)?, resolver, rate,
        )?;
        assert_eq!(reopened.export_authority_state()?, expected);
        let resumed = reopened.register_or_resume_exact(metadata, at)?;
        drop(resumed);
        reopened.shutdown()?;
        Ok(())
    }
}
