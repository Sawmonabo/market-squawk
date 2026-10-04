//! Exact account cleanup remains registry-owned until its completion is acknowledged.

use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) struct RetainedAccountStop {
    request: PreparedMarketProviderConfigurationRequest,
    generation: MarketRuntimeGroupGeneration,
    owner: Mutex<MarketRuntimeEntry>,
    complete: AtomicBool,
}

/// Runtime-minted completion for this exact retained allocation, never a serialized authority.
#[derive(Clone)]
pub(crate) struct AccountGroupStopReceipt {
    retained: Arc<RetainedAccountStop>,
}

impl fmt::Debug for AccountGroupStopReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AccountGroupStopReceipt")
            .field("generation", &self.retained.generation)
            .field("complete", &self.retained.complete.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl AccountGroupStopReceipt {
    pub(crate) fn generation(&self) -> MarketRuntimeGroupGeneration {
        self.retained.generation
    }

    pub(crate) fn request(&self) -> PreparedMarketProviderConfigurationRequest {
        self.retained.request
    }
}

/// Non-cloneable observation of this registry's actual allocation; contains no new authority.
pub(crate) struct PreparedAccountStop {
    surface: AccountMarketSurface,
    predecessor: Option<(
        PreparedMarketProviderConfigurationRequest,
        MarketRuntimeGroupGeneration,
    )>,
}

impl PreparedAccountStop {
    pub(crate) fn predecessor(
        &self,
    ) -> Option<(
        PreparedMarketProviderConfigurationRequest,
        MarketRuntimeGroupGeneration,
    )> {
        self.predecessor
    }
}

impl RetainedAccountStop {
    fn matches(
        &self,
        request: Option<PreparedMarketProviderConfigurationRequest>,
        expected: Option<MarketRuntimeGroupGeneration>,
    ) -> bool {
        request.is_none_or(|request| request == self.request)
            && expected.is_none_or(|expected| expected == self.generation)
    }

    async fn finish(
        &self,
        registry: &MarketRuntimeRegistry,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        if self.complete.load(Ordering::Acquire) {
            return Ok(());
        }
        // A recovery operation also prepares its successor. Physical cleanup retains its
        // own shorter shutdown bound without consuming that operation's entire lifetime.
        let deadline = deadline.min(registry.cleanup_deadline()?);
        let mut entry = bounded_lock(&self.owner, deadline, cancellation)
            .await
            .inspect_err(|error| {
                tracing::warn!(%error, stage = "retained-owner",
                    generation = ?self.generation.digest(), "account stop remains incomplete");
            })?;
        if self.complete.load(Ordering::Acquire) {
            return Ok(());
        }
        registry
            .clear_durable_market_routes(&entry.surface_id, deadline, cancellation)
            .await
            .inspect_err(|error| {
                tracing::warn!(%error, stage = "durable-routes",
                    generation = ?self.generation.digest(), "account stop remains incomplete");
            })?;
        let MarketRuntime::Account(group) = &mut entry.runtime else {
            return Err(ServiceError::InvalidResult);
        };
        group
            .finish_published_before(&registry.alpaca_historical_source, deadline, cancellation)
            .await
            .inspect_err(|error| {
                tracing::warn!(%error, stage = "published-group",
                    generation = ?self.generation.digest(), "account stop remains incomplete");
            })?;
        self.complete.store(true, Ordering::Release);
        Ok(())
    }
}

impl MarketRuntimeRegistry {
    /// Observes the actual owner without cancelling it or removing any registry entry.
    pub(crate) async fn prepare_account_stop(
        &self,
        surface: AccountMarketSurface,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PreparedAccountStop, ServiceError> {
        self.finish_account_start_before(surface, deadline, cancellation)
            .await?;
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        self.prepare_account_stop_owned(surface, deadline, cancellation)
            .await
    }

    pub(super) async fn prepare_account_stop_owned(
        &self,
        surface: AccountMarketSurface,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PreparedAccountStop, ServiceError> {
        if bounded_lock(&self.account_starts, deadline, cancellation)
            .await?
            .iter()
            .any(|start| start.request.surface() == surface)
        {
            return Err(ServiceError::Unavailable);
        }
        let stops = bounded_lock(&self.account_stops, deadline, cancellation).await?;
        if let Some(stop) = stops.iter().find(|stop| stop.request.surface() == surface) {
            return Ok(PreparedAccountStop {
                surface,
                predecessor: Some((stop.request, stop.generation)),
            });
        }
        let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
        let predecessor = entries
            .iter()
            .find(|entry| entry.surface_id.as_str() == surface.surface_id())
            .map(|entry| {
                let evidence = entry
                    .runtime
                    .account_evidence()
                    .ok_or(ServiceError::InvalidResult)?;
                let request = PreparedMarketProviderConfigurationRequest::try_new(
                    surface,
                    evidence.onboarding_session_id(),
                    evidence.public_configuration_digest(),
                    evidence.runtime_verification_receipt_digest(),
                    evidence.credential_generation(),
                )?;
                Ok::<_, ServiceError>((request, evidence.group_generation()))
            })
            .transpose()?;
        Ok(PreparedAccountStop {
            surface,
            predecessor,
        })
    }

    /// The lifecycle owner persists the preparation's exact coordinates before consuming it.
    /// Rechecks absence as well as presence under mutation; a concurrent replacement never passes.
    pub(crate) async fn consume_account_stop(
        &self,
        prepared: PreparedAccountStop,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<AccountGroupStopReceipt>, ServiceError> {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        let actual = self
            .prepare_account_stop_owned(prepared.surface, deadline, cancellation)
            .await?;
        if actual.predecessor != prepared.predecessor {
            return Err(ServiceError::InvalidRequest);
        }
        let Some((request, generation)) = prepared.predecessor else {
            return Ok(None);
        };
        let retained = self
            .retain_account_stop_owned(
                prepared.surface,
                Some(request),
                Some(generation),
                deadline,
                cancellation,
            )
            .await?
            .ok_or(ServiceError::InvalidResult)?;
        self.finish_account_stop_owned(&retained, deadline, cancellation)
            .await?;
        Ok(Some(AccountGroupStopReceipt { retained }))
    }

    /// OAuth validation observes the same actual owner as ordinary lifecycle preparation.
    pub(crate) async fn prepare_schwab_oauth_stop(
        &self,
        session_id: uuid::Uuid,
        current: Option<SchwabOAuthAuthorityReceipt>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PreparedAccountStop, ServiceError> {
        if session_id.is_nil() {
            return Err(ServiceError::InvalidRequest);
        }
        self.finish_account_start_before(
            AccountMarketSurface::SchwabMarketData,
            deadline,
            cancellation,
        )
        .await?;
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        let expected = self
            .schwab_stop_generation(session_id, current, deadline, cancellation)
            .await?;
        let prepared = self
            .prepare_account_stop_owned(
                AccountMarketSurface::SchwabMarketData,
                deadline,
                cancellation,
            )
            .await?;
        if prepared.predecessor.map(|(_, generation)| generation) != expected {
            return Err(ServiceError::InvalidResult);
        }
        Ok(prepared)
    }

    /// Caller holds mutation. Acquire both containers before moving ownership; no await occurs
    /// between removal from active entries and insertion into this bounded retained slot.
    pub(super) async fn retain_account_stop_owned(
        &self,
        surface: AccountMarketSurface,
        request: Option<PreparedMarketProviderConfigurationRequest>,
        expected: Option<MarketRuntimeGroupGeneration>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<Arc<RetainedAccountStop>>, ServiceError> {
        let surface_id = try_surface_identifier(surface)?;
        let mut stops = bounded_lock(&self.account_stops, deadline, cancellation).await?;
        if let Some(retained) = stops.iter().find(|stop| stop.request.surface() == surface) {
            if !retained.matches(request, expected) {
                return Err(ServiceError::InvalidRequest);
            }
            return Ok(Some(Arc::clone(retained)));
        }
        let mut entries = bounded_lock(&self.entries, deadline, cancellation).await?;
        let Some(index) = entries
            .iter()
            .position(|entry| entry.surface_id == surface_id)
        else {
            return Ok(None);
        };
        let entry = &entries[index];
        if entry.exports.is_some() || entry.action_hooks_installed {
            return Err(ServiceError::InvalidResult);
        }
        let evidence = entry
            .runtime
            .account_evidence()
            .ok_or(ServiceError::InvalidRequest)?;
        let actual = PreparedMarketProviderConfigurationRequest::try_new(
            surface,
            evidence.onboarding_session_id(),
            evidence.public_configuration_digest(),
            evidence.runtime_verification_receipt_digest(),
            evidence.credential_generation(),
        )?;
        if request.is_some_and(|request| request != actual)
            || expected.is_some_and(|expected| expected != evidence.group_generation())
        {
            return Err(ServiceError::InvalidRequest);
        }
        let generation = evidence.group_generation();
        if stops.len() >= MAXIMUM_CONCURRENT_MARKET_SURFACES {
            return Err(ServiceError::ResourceExhausted);
        }
        stops
            .try_reserve(1)
            .map_err(|_| ServiceError::ResourceExhausted)?;
        entry.begin_shutdown();
        let retained = Arc::new(RetainedAccountStop {
            request: actual,
            generation,
            owner: Mutex::new(entries.swap_remove(index)),
            complete: AtomicBool::new(false),
        });
        stops.push(Arc::clone(&retained));
        Ok(Some(retained))
    }

    pub(super) async fn finish_account_stop_owned(
        &self,
        retained: &Arc<RetainedAccountStop>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketRuntimeGroupGeneration, ServiceError> {
        retained.finish(self, deadline, cancellation).await?;
        Ok(retained.generation)
    }

    pub(super) async fn ensure_account_stop_acknowledged(
        &self,
        surface: AccountMarketSurface,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let stops = bounded_lock(&self.account_stops, deadline, cancellation).await?;
        if stops.iter().any(|stop| stop.request.surface() == surface) {
            return Err(ServiceError::Unavailable);
        }
        Ok(())
    }

    /// Runs the owning lifecycle's exact durable CAS before removing this completed allocation.
    /// There is no cancellation point between successful persistence and acknowledgement.
    pub(crate) async fn acknowledge_account_group_stop<Commit>(
        &self,
        receipt: &AccountGroupStopReceipt,
        commit: Commit,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError>
    where
        Commit: FnOnce(&AccountGroupStopReceipt) -> Result<(), ServiceError>,
    {
        let _mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        let mut stops = bounded_lock(&self.account_stops, deadline, cancellation).await?;
        let Some(index) = stops
            .iter()
            .position(|stop| Arc::ptr_eq(stop, &receipt.retained))
        else {
            // Replaying an already committed acknowledgement never names a new allocation.
            if !receipt.retained.complete.load(Ordering::Acquire) {
                return Err(ServiceError::Unavailable);
            }
            return commit(receipt);
        };
        if !stops[index].complete.load(Ordering::Acquire) {
            return Err(ServiceError::Unavailable);
        }
        commit(receipt)?;
        stops.swap_remove(index);
        Ok(())
    }

    /// Product shutdown and the existing health worker join the same retained owners.
    pub(super) async fn finish_retained_account_stops(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let retained = bounded_lock(&self.account_stops, deadline, cancellation)
            .await?
            .clone();
        let mut failure = None;
        for stop in retained {
            if let Err(error) = stop.finish(self, deadline, cancellation).await {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

impl MarketRuntimeRegistry {
    pub(super) async fn schwab_stop_generation(
        &self,
        session_id: uuid::Uuid,
        current: Option<SchwabOAuthAuthorityReceipt>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<MarketRuntimeGroupGeneration>, ServiceError> {
        let retained = bounded_lock(&self.account_stops, deadline, cancellation)
            .await?
            .iter()
            .find(|stop| stop.request.surface() == AccountMarketSurface::SchwabMarketData)
            .cloned();
        if let Some(retained) = retained {
            let entry = bounded_lock(&retained.owner, deadline, cancellation).await?;
            return validate_schwab_oauth_entry(&entry, session_id, current).map(Some);
        }
        let entries = bounded_lock(&self.entries, deadline, cancellation).await?;
        entries
            .iter()
            .find(|entry| {
                entry.surface_id.as_str()
                    == crate::provider_onboarding::SCHWAB_MARKET_DATA_SURFACE_ID
            })
            .map(|entry| validate_schwab_oauth_entry(entry, session_id, current))
            .transpose()
    }
}

fn validate_schwab_oauth_entry(
    entry: &MarketRuntimeEntry,
    session_id: uuid::Uuid,
    current: Option<SchwabOAuthAuthorityReceipt>,
) -> Result<MarketRuntimeGroupGeneration, ServiceError> {
    if entry.onboarding_session_id != Some(session_id) {
        return Err(ServiceError::InvalidRequest);
    }
    let group = entry
        .runtime
        .account_evidence()
        .ok_or(ServiceError::InvalidRequest)?;
    let receipt = entry
        .runtime
        .account_activation_lease()
        .and_then(|lease| {
            lease
                .runtime_verification_evidence()
                .schwab_market_data_receipt()
        })
        .ok_or(ServiceError::InvalidRequest)?;
    if group.onboarding_session_id() != session_id
        || uuid::Uuid::parse_str(receipt.session_identifier().as_str()) != Ok(session_id)
        || current
            .is_some_and(|current| current.generation().get() < receipt.access_token_generation())
    {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(group.group_generation())
}

/// Sole in-flight constructor for one exact account request, retained before its first await.
pub(super) struct RetainedAccountStart {
    request: PreparedMarketProviderConfigurationRequest,
    cancellation: CancellationToken,
    worker: Mutex<AccountStartWorker>,
}

struct AccountStartWorker {
    task: tokio::task::JoinHandle<
        Result<MarketProviderGroupLifecycleEvidence, AccountRuntimeStartFailure>,
    >,
    joined: Option<Result<MarketProviderGroupLifecycleEvidence, AccountRuntimeStartFailure>>,
}

impl RetainedAccountStart {
    /// Outer errors are interrupted waits; inner errors are actual joined constructor outcomes.
    async fn wait(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<
        Result<MarketProviderGroupLifecycleEvidence, AccountRuntimeStartFailure>,
        ServiceError,
    > {
        let mut worker = bounded_lock(&self.worker, deadline, cancellation).await?;
        if let Some(result) = &worker.joined {
            return Ok(result.clone());
        }
        let joined = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ServiceError::Cancelled),
            () = tokio::time::sleep_until(deadline.into()) => return Err(ServiceError::DeadlineExceeded),
            joined = &mut worker.task => joined,
        };
        let result = joined.unwrap_or_else(|error| {
            tracing::error!(%error, "retained account constructor join failed");
            Err(AccountRuntimeStartFailure::after_cleanup(
                ServiceError::Unavailable,
                Err(ServiceError::Unavailable),
            ))
        });
        // The task is joined exactly once; no await separates completion from retaining it.
        worker.joined = Some(result.clone());
        Ok(result)
    }
}

impl MarketRuntimeRegistry {
    pub(super) async fn start_account_group_retained(
        self: &Arc<Self>,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<MarketProviderGroupLifecycleEvidence, ServiceError> {
        let mutation = bounded_lock(&self.mutation, deadline, cancellation).await?;
        ensure_active(&self.accepting, deadline, cancellation)?;
        let mut starts = bounded_lock(&self.account_starts, deadline, cancellation).await?;
        let retained = if let Some(start) = starts
            .iter()
            .find(|start| start.request.surface() == request.surface())
        {
            if start.request != request {
                return Err(ServiceError::InvalidRequest);
            }
            Arc::clone(start)
        } else {
            if starts.len() >= MAXIMUM_CONCURRENT_MARKET_SURFACES {
                return Err(ServiceError::ResourceExhausted);
            }
            starts
                .try_reserve(1)
                .map_err(|_| ServiceError::ResourceExhausted)?;
            let owned = Arc::clone(self);
            let lifecycle = self.account_start_cancellation.child_token();
            let task_cancellation = lifecycle.clone();
            let task = tokio::spawn(async move {
                // Preparation acquires mutation after this caller retains the actual handle.
                // The surface reservation, not the global lock, owns slow startup and cleanup.
                owned
                    .start_account_group_reserved(request, deadline, &task_cancellation)
                    .await
            });
            let retained = Arc::new(RetainedAccountStart {
                request,
                cancellation: lifecycle,
                worker: Mutex::new(AccountStartWorker { task, joined: None }),
            });
            starts.push(Arc::clone(&retained));
            retained
        };
        drop(starts);
        drop(mutation);
        let mut waiter = StartupCancellation::new(retained.cancellation.clone());
        let result = retained.wait(deadline, cancellation).await?;
        match result {
            Ok(evidence) => {
                waiter.disarm();
                self.release_completed_account_start(&retained, deadline, cancellation)
                    .await?;
                Ok(evidence)
            }
            Err(failure) => {
                if failure.cleanup.is_ok() {
                    self.release_completed_account_start(&retained, deadline, cancellation)
                        .await?;
                }
                Err(failure.cause)
            }
        }
    }

    /// Caller holds mutation. A slot cannot be released until its original task has joined,
    /// so this exact request cannot be replaced while its constructor prepares or publishes.
    pub(super) async fn require_account_start_reserved(
        &self,
        request: PreparedMarketProviderConfigurationRequest,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let starts = bounded_lock(&self.account_starts, deadline, cancellation).await?;
        if starts.iter().any(|start| start.request == request) {
            Ok(())
        } else {
            Err(ServiceError::InvalidRequest)
        }
    }

    async fn release_completed_account_start(
        &self,
        retained: &Arc<RetainedAccountStart>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let mut starts = bounded_lock(&self.account_starts, deadline, cancellation).await?;
        if let Some(index) = starts.iter().position(|start| Arc::ptr_eq(start, retained)) {
            starts.swap_remove(index);
        }
        Ok(())
    }

    /// Called before registry mutation: the original constructor itself needs mutation to finish.
    pub(super) async fn finish_account_start_before(
        &self,
        surface: AccountMarketSurface,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let retained = bounded_lock(&self.account_starts, deadline, cancellation)
            .await?
            .iter()
            .find(|start| start.request.surface() == surface)
            .cloned();
        if let Some(retained) = retained {
            match retained.wait(deadline, cancellation).await? {
                Ok(_) => {}
                Err(failure) => {
                    failure.cleanup?;
                }
            }
            self.release_completed_account_start(&retained, deadline, cancellation)
                .await?;
        }
        Ok(())
    }

    /// Health and product shutdown join the same task; only actual cleanup success releases it.
    pub(super) async fn finish_retained_account_starts(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let starts = bounded_lock(&self.account_starts, deadline, cancellation)
            .await?
            .clone();
        let mut failure = None;
        for start in starts {
            if let Err(error) = self
                .finish_account_start_before(start.request.surface(), deadline, cancellation)
                .await
            {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}
