//! Debug-only installed-composition fixture for the exact Federal Reserve Board H.15 vertical.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use market_squawk_adapter_federal_reserve::{
    BoardScriptedDoctorExecutor, BoardScriptedTransportCounters, BoardScriptedTransportFactory,
};
use market_squawk_data::SqliteProviderRateStore;
use market_squawk_domain::Timestamp;
use market_squawk_sources::{ProviderRateAuthority, ProviderRateStore, ProviderRateStoreError};

use crate::provider_rate::open_provider_rate_store;

/// Closed debug-only Board transport and provider-rate composition shared by one installed run.
///
/// The scripted transport replaces only exact HTTP execution. The normal code-owned profile,
/// onboarding lifecycle, activation recipe, extraction registry, raw-capture store, catalog,
/// Parquet publication, and typed reads remain unchanged. One bundle binds at most one workspace
/// root and retains exactly one owner-held SQLite store and manually observed rate authority.
#[derive(Clone, Debug)]
pub struct BoardInstalledFixtureBundle {
    inner: Arc<BoardInstalledFixtureInner>,
}

#[derive(Debug)]
struct BoardInstalledFixtureInner {
    transport: BoardScriptedTransportFactory,
    initial_wall_clock: Timestamp,
    rate: Mutex<BoardInstalledFixtureRate>,
}

#[derive(Debug)]
enum BoardInstalledFixtureRate {
    Unbound,
    Bound {
        control_root: PathBuf,
        _store: Arc<SqliteProviderRateStore>,
        authority: ProviderRateAuthority,
        manual_wall_clock: Timestamp,
    },
}

impl BoardInstalledFixtureBundle {
    /// Retains one closed scripted doctor/production transport and initial paired rate clock.
    #[must_use]
    pub fn new(transport: BoardScriptedTransportFactory, initial_wall_clock: Timestamp) -> Self {
        Self {
            inner: Arc::new(BoardInstalledFixtureInner {
                transport,
                initial_wall_clock,
                rate: Mutex::new(BoardInstalledFixtureRate::Unbound),
            }),
        }
    }

    /// Returns exact doctor/production execution counters from the shared scripted transport.
    #[must_use]
    pub fn transport_counters(&self) -> BoardScriptedTransportCounters {
        self.inner.transport.counters()
    }

    /// Advances the already bound durable authority's paired clock by exactly `duration`.
    ///
    /// # Errors
    ///
    /// Fails when installed composition has not bound the fixture, synchronization failed, or the
    /// manual clock cannot represent the requested advance.
    pub fn advance_provider_clock(&self, duration: Duration) -> Result<(), ProviderRateStoreError> {
        let mut rate = self
            .inner
            .rate
            .lock()
            .map_err(|_| ProviderRateStoreError::Corrupt)?;
        let BoardInstalledFixtureRate::Bound {
            authority,
            manual_wall_clock,
            ..
        } = &mut *rate
        else {
            return Err(ProviderRateStoreError::Unavailable);
        };
        let nanos =
            i64::try_from(duration.as_nanos()).map_err(|_| ProviderRateStoreError::Clock)?;
        let advanced = manual_wall_clock
            .checked_add_nanos(nanos)
            .map_err(|_| ProviderRateStoreError::Clock)?;
        authority.advance_debug_manual_clock(duration)?;
        *manual_wall_clock = advanced;
        Ok(())
    }

    /// Moves the retained paired clock forward to the observed wall only after a clean test stop.
    ///
    /// Durable source checkpoints are anchored to trusted system time. This fixture-only bridge
    /// preserves the same store and authority while preventing the deliberately stopped manual
    /// clock from appearing to roll back when that exact workspace is reopened.
    ///
    /// # Errors
    ///
    /// Rejects an unbound fixture, wall-clock rollback or overflow, synchronization failure, or
    /// an unavailable manual provider-rate clock.
    pub fn synchronize_provider_clock_for_restart(&self) -> Result<(), ProviderRateStoreError> {
        let elapsed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| ProviderRateStoreError::Clock)?;
        let observed_nanos =
            i64::try_from(elapsed.as_nanos()).map_err(|_| ProviderRateStoreError::Clock)?;
        let observed = Timestamp::from_unix_nanos(observed_nanos);
        let mut rate = self
            .inner
            .rate
            .lock()
            .map_err(|_| ProviderRateStoreError::Corrupt)?;
        let BoardInstalledFixtureRate::Bound {
            authority,
            manual_wall_clock,
            ..
        } = &mut *rate
        else {
            return Err(ProviderRateStoreError::Unavailable);
        };
        let elapsed_nanos = observed
            .unix_nanos()
            .checked_sub(manual_wall_clock.unix_nanos())
            .ok_or(ProviderRateStoreError::Clock)?;
        let elapsed_nanos =
            u64::try_from(elapsed_nanos).map_err(|_| ProviderRateStoreError::Clock)?;
        if elapsed_nanos == 0 {
            return Ok(());
        }
        authority.advance_debug_manual_clock(Duration::from_nanos(elapsed_nanos))?;
        *manual_wall_clock = observed;
        Ok(())
    }

    pub(crate) fn doctor_executor(&self) -> BoardScriptedDoctorExecutor {
        self.inner.transport.doctor_executor()
    }

    pub(crate) fn production_source_factory(&self) -> BoardScriptedTransportFactory {
        self.inner.transport.clone()
    }

    pub(crate) fn bind_provider_rate(
        &self,
        control_root: &Path,
    ) -> Result<ProviderRateAuthority, ProviderRateStoreError> {
        let mut rate = self
            .inner
            .rate
            .lock()
            .map_err(|_| ProviderRateStoreError::Corrupt)?;
        match &*rate {
            BoardInstalledFixtureRate::Bound {
                control_root: bound,
                authority,
                ..
            } if bound == control_root => return Ok(authority.clone()),
            BoardInstalledFixtureRate::Bound { .. } => {
                return Err(ProviderRateStoreError::Conflict);
            }
            BoardInstalledFixtureRate::Unbound => {}
        }
        let store = open_provider_rate_store(control_root)?;
        let rate_store: Arc<dyn ProviderRateStore> = store.clone();
        let authority = ProviderRateAuthority::try_new_with_debug_manual_clock(
            rate_store,
            self.inner.initial_wall_clock,
        )?;
        *rate = BoardInstalledFixtureRate::Bound {
            control_root: control_root.to_path_buf(),
            _store: store,
            authority: authority.clone(),
            manual_wall_clock: self.inner.initial_wall_clock,
        };
        Ok(authority)
    }
}
