//! Query-only catalog diagnostics for local control-plane inspection.

use market_squawk_sources::OnboardingState;
use rusqlite::{Connection, OpenFlags};
use uuid::Uuid;

use super::storage::{CATALOG_APPLICATION_ID, prepare_local_path, verify_migration_identities};
use super::{Catalog, CatalogConfig, CatalogError, CatalogLimit, map_catalog_location_error};

/// Bounded, query-only facts from one exact local catalog snapshot.
#[derive(Debug)]
pub struct CatalogDiagnosticSnapshot {
    journal_mode: String,
    applied_migrations: u32,
    current_provider_sessions: Vec<ProviderOnboardingDiagnostic>,
}

impl CatalogDiagnosticSnapshot {
    /// Returns the normalized durable journal mode.
    pub fn journal_mode(&self) -> &str {
        &self.journal_mode
    }

    /// Returns the number of digest-verified applied migrations.
    pub const fn applied_migrations(&self) -> u32 {
        self.applied_migrations
    }

    /// Returns the latest fully replayed onboarding session for each retained surface.
    pub fn current_provider_sessions(&self) -> &[ProviderOnboardingDiagnostic] {
        &self.current_provider_sessions
    }
}

/// Secret-free state from one fully replayed provider-onboarding session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderOnboardingDiagnostic {
    surface_id: String,
    session_id: Uuid,
    state: OnboardingState,
}

impl ProviderOnboardingDiagnostic {
    pub(super) fn new(surface_id: String, session_id: Uuid, state: OnboardingState) -> Self {
        Self {
            surface_id,
            session_id,
            state,
        }
    }

    /// Returns the exact code-owned provider surface.
    pub fn surface_id(&self) -> &str {
        &self.surface_id
    }

    /// Returns the opaque durable onboarding-session identity.
    pub const fn session_id(&self) -> Uuid {
        self.session_id
    }

    /// Returns the fully replayed current lifecycle state.
    pub const fn state(&self) -> OnboardingState {
        self.state
    }
}

impl Catalog {
    /// Reads current catalog and provider-onboarding facts without acquiring writer authority.
    ///
    /// The connection is opened with SQLite's read-only flag and `query_only` defense in depth.
    /// It participates in normal WAL locking so a concurrent writer cannot be bypassed or observed
    /// through an unsafe immutable snapshot. No schema initialization, migration, checkpoint,
    /// recovery publication, or Market Squawk writer lock is attempted.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError`] when the existing catalog is unsafe, unavailable, not at the exact
    /// migration set, or contains a provider lifecycle that cannot be replayed within bounds.
    pub fn diagnostics(
        config: CatalogConfig,
        provider_limit: CatalogLimit,
    ) -> Result<CatalogDiagnosticSnapshot, CatalogError> {
        if provider_limit.get() > config.max_result_rows.get() {
            return Err(CatalogError::InvalidLimit);
        }
        config
            .location
            .validate_for_open()
            .map_err(map_catalog_location_error)?;
        let catalog_file = config
            .location
            .open_catalog_file()
            .map_err(map_catalog_location_error)?;
        let path = prepare_local_path(config.location.path())?;
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW;
        let connection = Connection::open_with_flags(path, flags)?;
        connection.busy_timeout(config.busy_timeout)?;
        connection.pragma_update(None, "query_only", true)?;
        connection.pragma_update(None, "trusted_schema", "OFF")?;
        catalog_file
            .validate_identity()
            .map_err(map_catalog_location_error)?;
        let application_id: i64 =
            connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
        if application_id != CATALOG_APPLICATION_ID {
            return Err(CatalogError::ForeignCatalog);
        }
        verify_migration_identities(&connection)?;
        let journal_mode = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))?
            .to_ascii_lowercase();
        if journal_mode != "wal" {
            return Err(CatalogError::UnsafeJournalMode);
        }
        let applied_migrations: i64 =
            connection.query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
                row.get(0)
            })?;
        let applied_migrations =
            u32::try_from(applied_migrations).map_err(|_| CatalogError::CorruptCatalog)?;
        let current_provider_sessions = super::onboarding::diagnostic_current_sessions(
            &connection,
            provider_limit,
            config.result_bytes,
        )?;
        catalog_file
            .validate_identity()
            .map_err(map_catalog_location_error)?;
        config
            .location
            .validate_for_open()
            .map_err(map_catalog_location_error)?;
        Ok(CatalogDiagnosticSnapshot {
            journal_mode,
            applied_migrations,
            current_provider_sessions,
        })
    }
}
