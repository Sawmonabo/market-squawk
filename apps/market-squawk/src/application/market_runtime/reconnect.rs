//! The existing account-health worker requests exact durable lifecycle recovery.

use super::*;
use async_trait::async_trait;

#[async_trait]
pub(crate) trait AccountMarketRuntimeReconnect: Send + Sync {
    /// A completed public producer can request one exact stale-selection recovery.
    async fn reconnect_public(
        &self,
        provider: SourceIdentifier,
        session: uuid::Uuid,
        incarnation: uuid::Uuid,
        cancellation: CancellationToken,
    ) -> Result<(), ServiceError>;

    /// Startup needs this worker only when original unfinished recovery intent exists.
    async fn has_pending(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<bool, ServiceError>;

    /// Revisit the original durable intent after interrupted cleanup or successor startup.
    async fn resume_pending(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), ServiceError>;

    /// Recheck the original coordinates before persisting recovery intent.
    async fn reconnect(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        generation: MarketRuntimeGroupGeneration,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), ServiceError>;
}

impl MarketRuntimeRegistry {
    pub(crate) async fn bind_account_reconnect(
        self: &Arc<Self>,
        owner: Weak<dyn AccountMarketRuntimeReconnect>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let pending = owner
            .upgrade()
            .ok_or(ServiceError::Unavailable)?
            .has_pending(deadline, cancellation.child_token())
            .await?;
        self.account_reconnect
            .set(owner)
            .map_err(|_| ServiceError::InvalidRequest)?;
        // Restored pending intent may lack both an entry and a current doctor lease, so it
        // cannot rely on reaching start_account_group to start this existing worker.
        if pending {
            self.ensure_account_health_drain_started(deadline, cancellation)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn recover_unhealthy_account_groups(&self, cancellation: &CancellationToken) {
        let Ok(deadline) = self.cleanup_deadline() else {
            return;
        };
        let snapshots = match self.unhealthy_account_groups(deadline, cancellation).await {
            Ok(snapshots) => snapshots,
            Err(error) => {
                if !cancellation.is_cancelled() {
                    tracing::warn!(%error, "account health snapshot unavailable");
                }
                return;
            }
        };
        // A blocked Schwab renewal must not repeatedly select the first entry and starve
        // independent account cleanup. Every observed unrelated generation gets its own turn.
        for snapshot in snapshots.iter().filter(|snapshot| {
            snapshot.surface_id.as_str() != AccountMarketSurface::SchwabMarketData.surface_id()
        }) {
            self.drain_unhealthy_account_snapshot(snapshot, cancellation)
                .await;
        }
        let Ok(deadline) = self.cleanup_deadline() else {
            return;
        };
        if let Some(owner) = self.account_reconnect.get().and_then(Weak::upgrade)
            && let Err(error) = owner
                .resume_pending(deadline, cancellation.child_token())
                .await
            && !cancellation.is_cancelled()
        {
            tracing::warn!(%error, "pending Schwab lifecycle recovery remains incomplete");
        }
        for snapshot in snapshots.iter().filter(|snapshot| {
            snapshot.surface_id.as_str() == AccountMarketSurface::SchwabMarketData.surface_id()
        }) {
            let Ok(deadline) = self.cleanup_deadline() else {
                return;
            };
            match self
                .reconnect_account_group_generation(snapshot, deadline, cancellation)
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    self.drain_unhealthy_account_snapshot(snapshot, cancellation)
                        .await;
                }
                Err(error) => {
                    // Recovery retains the original allocation; generic removal cannot
                    // replace its persisted predecessor or manufacture acknowledgement.
                    if !cancellation.is_cancelled() {
                        tracing::warn!(%error, "Schwab generation recovery remains incomplete");
                    }
                }
            }
        }
    }

    async fn drain_unhealthy_account_snapshot(
        &self,
        snapshot: &AccountMarketRuntimeHealthSnapshot,
        cancellation: &CancellationToken,
    ) {
        if let Err(error) = self.drain_account_group_generation(snapshot).await
            && !cancellation.is_cancelled()
        {
            tracing::error!(
                %error,
                surface = %snapshot.surface_id.as_str(),
                generation = ?snapshot.group_generation.digest(),
                "account-market stale generation drain failed"
            );
        }
    }

    /// True means the lifecycle owner handled this notification, including a stale one.
    async fn reconnect_account_group_generation(
        &self,
        snapshot: &AccountMarketRuntimeHealthSnapshot,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<bool, ServiceError> {
        ensure_active(&self.accepting, deadline, cancellation)?;
        let Some(owner) = self.account_reconnect.get().and_then(Weak::upgrade) else {
            return Ok(false);
        };
        let request = {
            let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            let Some(entry) = entries.iter().find(|entry| {
                matches_unhealthy_account_generation(
                    &snapshot.surface_id,
                    snapshot.group_generation.digest(),
                    &entry.surface_id,
                    entry
                        .runtime
                        .account_evidence()
                        .map(|e| e.group_generation().digest()),
                    entry.is_healthy(),
                )
            }) else {
                return Ok(true);
            };
            let MarketRuntime::Account(group) = &entry.runtime else {
                return Err(ServiceError::InvalidResult);
            };
            if group.schwab_recovery_owner().is_none() {
                return Ok(false);
            }
            let evidence = group.evidence();
            PreparedMarketProviderConfigurationRequest::try_new(
                AccountMarketSurface::SchwabMarketData,
                evidence.onboarding_session_id(),
                evidence.public_configuration_digest(),
                evidence.runtime_verification_receipt_digest(),
                evidence.credential_generation(),
            )?
        };
        // No registry lock spans the lifecycle gate, OAuth continuation or physical drain.
        owner
            .reconnect(
                request,
                snapshot.group_generation,
                deadline,
                cancellation.child_token(),
            )
            .await?;
        Ok(true)
    }
}

impl MarketRuntimeRegistry {
    /// Only completion notifications enter this path; the account timer never retries it.
    pub(super) async fn recover_completed_public_sources(&self, cancellation: &CancellationToken) {
        let Some(owner) = self.account_reconnect.get().and_then(Weak::upgrade) else {
            return;
        };
        let Ok(deadline) = self.cleanup_deadline() else {
            return;
        };
        let snapshots = {
            let Ok(entries) = bounded_lock(&self.entries, deadline, cancellation).await else {
                return;
            };
            let mut snapshots = Vec::new();
            if snapshots.try_reserve_exact(entries.len()).is_err() {
                return;
            }
            for entry in entries.iter() {
                if let MarketRuntime::Public(runtime) = &entry.runtime
                    && let Some(incarnation) = runtime.completed_incarnation()
                    && let Some(session) = entry.onboarding_session_id
                {
                    snapshots.push((entry.surface_id.clone(), session, incarnation));
                }
            }
            snapshots
        };
        for (provider, session, incarnation) in snapshots {
            if cancellation.is_cancelled() {
                return;
            }
            if let Err(error) = owner
                .reconnect_public(provider, session, incarnation, cancellation.child_token())
                .await
                && !cancellation.is_cancelled()
            {
                tracing::warn!(%error, "public catalog-selection recovery remains blocked");
            }
        }
    }

    /// Consume only the notified predecessor. No runtime replacement or healthy generation
    /// can be selected by this operation, even when a completion notification was delayed.
    pub(crate) async fn prepare_public_recovery(
        &self,
        provider: &SourceIdentifier,
        session: uuid::Uuid,
        incarnation: uuid::Uuid,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<Duration>, ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        ensure_active(&self.accepting, deadline, cancellation)?;
        let (entry, delay) = {
            let mut entries = bounded_lock(&self.entries, deadline, cancellation).await?;
            let Some(index) = entries.iter().position(|entry| {
                &entry.surface_id == provider
                    && entry.onboarding_session_id == Some(session)
                    && matches!(&entry.runtime, MarketRuntime::Public(runtime)
                        if runtime.completed_incarnation() == Some(incarnation))
            }) else {
                return Ok(None);
            };
            if entries[index].action_hooks_installed {
                return Err(ServiceError::Unavailable);
            }
            let mut delay = Duration::ZERO;
            for source in entries[index].metadata.iter() {
                let policy = source.budget_policy().ok_or(ServiceError::Unavailable)?;
                // Catalog invalidation is not a provider refusal: use the registered delay
                // without altering shared provider refusal accounting.
                delay = delay.max(Duration::from_nanos(policy.backoff().delay_nanos(0, 0)));
            }
            (entries.swap_remove(index), delay)
        };
        // Once consumed, a non-stale or incomplete cleanup cannot become a generic retry
        // on the next account tick or a duplicate completion notification.
        entry
            .shutdown_public_for_reprepare(self.config.source_shutdown())
            .await?;
        Ok(Some(delay))
    }
}
