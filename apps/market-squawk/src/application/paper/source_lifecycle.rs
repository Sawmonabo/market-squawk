//! Source lifecycle control sharing the paper controller's sole live-runtime owner.

use std::{sync::Arc, time::Instant};

use market_squawk_domain::{
    ConnectionGeneration, CoverageStatus, DataQuality, SourceIdentifier, StreamIntegrityState,
    Timestamp,
};
use market_squawk_services::ServiceError;
use tokio_util::sync::CancellationToken;

use super::{PaperController, PaperProvider, PaperState};
use crate::ProductionSourceProvider;

/// Exact live evidence returned after a paper-owned source lifecycle operation.
#[derive(Clone, Debug)]
pub(crate) struct PaperSourceLifecycleEvidence {
    pub(crate) provider: SourceIdentifier,
    pub(crate) generation: ConnectionGeneration,
    pub(crate) coverage: CoverageStatus,
    pub(crate) integrity: StreamIntegrityState,
    pub(crate) quality: DataQuality,
    pub(crate) observed_at: Timestamp,
}

/// Least-authority lifecycle handle sharing one [`PaperController`].
#[derive(Clone, Debug)]
pub(crate) struct PaperSourceLifecycleControl {
    controller: Arc<PaperController>,
}

impl PaperSourceLifecycleControl {
    pub(super) const fn new(controller: Arc<PaperController>) -> Self {
        Self { controller }
    }

    /// Returns the exact number of currently healthy paper-owned live source runtimes.
    pub(crate) fn active_source_count(&self) -> Result<usize, ServiceError> {
        let state = self
            .controller
            .state
            .try_lock()
            .map_err(|_busy| ServiceError::Unavailable)?;
        match &*state {
            PaperState::Stopped { .. } => Ok(0),
            PaperState::LiveStarting { .. }
            | PaperState::Starting { .. }
            | PaperState::Stopping => Err(ServiceError::Unavailable),
            PaperState::LiveOnly {
                runtime,
                exports,
                cancellation,
                ..
            } if !cancellation.is_cancelled() && runtime.is_healthy() && exports.is_healthy() => {
                Ok(1)
            }
            PaperState::LiveOnly { .. } => Ok(0),
            PaperState::Running {
                runtime,
                exports,
                cancellation,
                ..
            } if !cancellation.is_cancelled()
                && runtime.source_is_healthy()
                && exports.is_healthy() =>
            {
                Ok(1)
            }
            PaperState::Running { .. } => Ok(0),
        }
    }

    /// Returns actual current runtime evidence for the exact provider surface.
    pub(crate) async fn verify(
        &self,
        provider: &SourceIdentifier,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<PaperSourceLifecycleEvidence>, ServiceError> {
        let state = super::bounded_lock(&self.controller.state, deadline, cancellation).await?;
        match &*state {
            PaperState::Stopped { .. } => Ok(None),
            PaperState::LiveStarting { .. }
            | PaperState::Starting { .. }
            | PaperState::Stopping => Err(ServiceError::Unavailable),
            PaperState::LiveOnly {
                provider: current,
                runtime,
                exports,
                cancellation: runtime_cancellation,
                ..
            } => {
                if !provider_matches(provider, PaperProvider::Public(*current)) {
                    return Err(ServiceError::InvalidRequest);
                }
                if runtime_cancellation.is_cancelled()
                    || !runtime.is_healthy()
                    || !exports.is_healthy()
                {
                    return Err(ServiceError::Unavailable);
                }
                aggregate(provider.clone(), runtime.snapshots())
            }
            PaperState::Running {
                provider: current,
                runtime,
                exports,
                cancellation: runtime_cancellation,
                ..
            } => {
                if !provider_matches(provider, *current) {
                    return Err(ServiceError::InvalidRequest);
                }
                if runtime_cancellation.is_cancelled()
                    || !runtime.source_is_healthy()
                    || !exports.is_healthy()
                {
                    return Err(ServiceError::Unavailable);
                }
                aggregate(provider.clone(), runtime.snapshots())
            }
        }
    }

    /// Stops the exact live owner without disturbing another selected public source.
    pub(crate) async fn stop(
        &self,
        provider: &SourceIdentifier,
        expected_generation: Option<ConnectionGeneration>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ConnectionGeneration>, ServiceError> {
        let _owner =
            super::bounded_lock(&self.controller.owner_gate, deadline, cancellation).await?;
        self.stop_owned(provider, expected_generation, true, deadline, cancellation)
            .await
    }

    async fn stop_owned(
        &self,
        provider: &SourceIdentifier,
        expected_generation: Option<ConnectionGeneration>,
        restore_other_public_source: bool,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ConnectionGeneration>, ServiceError> {
        let current = self.current_provider_owned(deadline, cancellation).await?;
        let Some(current) = current else {
            self.clear_selected_public_source_owned(provider, deadline, cancellation)
                .await?;
            return Ok(None);
        };
        if !provider_matches(provider, current) {
            // A different source owns the sole runtime. Stopping this retained, inactive surface
            // clears its restore selection but must not tear down the unrelated owner.
            self.clear_selected_public_source_owned(provider, deadline, cancellation)
                .await?;
            return Ok(None);
        };
        let previous = self
            .owned_generation(provider, deadline, cancellation)
            .await?;
        if expected_generation.is_some() && previous != expected_generation {
            return Err(ServiceError::InvalidRequest);
        }
        let restore_public_source = if restore_other_public_source {
            self.other_selected_public_source_owned(provider, deadline, cancellation)
                .await?
        } else {
            None
        };
        if !self
            .controller
            .stop_runtime_before_owned(deadline, cancellation)
            .await?
        {
            return Err(ServiceError::Unavailable);
        }
        if let Some(restore) = restore_public_source {
            let _compatible = self
                .compatible_restart_request_owned(PaperProvider::Public(restore))
                .await?;
            self.controller
                .start_public_source_owned(restore, deadline, cancellation)
                .await?;
        }
        Ok(previous)
    }

    /// Starts the exact source, reusing only compatible paper restart authority when retained.
    pub(crate) async fn start(
        &self,
        provider: &SourceIdentifier,
        onboarding_session_id: Option<uuid::Uuid>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PaperSourceLifecycleEvidence, ServiceError> {
        let _owner =
            super::bounded_lock(&self.controller.owner_gate, deadline, cancellation).await?;
        self.start_owned(provider, onboarding_session_id, deadline, cancellation)
            .await
    }

    async fn start_owned(
        &self,
        provider: &SourceIdentifier,
        onboarding_session_id: Option<uuid::Uuid>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PaperSourceLifecycleEvidence, ServiceError> {
        let expected = provider_kind(provider, onboarding_session_id)?;
        if let Some(current_provider) = self.current_provider_owned(deadline, cancellation).await? {
            if exact_provider(current_provider, expected) {
                match self.verify(provider, deadline, cancellation).await {
                    Ok(Some(current)) => {
                        let _compatible = self.compatible_restart_request_owned(expected).await?;
                        self.mark_selected_source_owned(expected, deadline, cancellation)
                            .await?;
                        return Ok(current);
                    }
                    Ok(None) | Err(ServiceError::Unavailable) => {}
                    Err(error) => return Err(error),
                }
            }
            // The one-owner gate remains held while the old runtime is completely retired and the
            // requested provider takes ownership, so no second connection can enter the gap.
            if !self
                .controller
                .stop_runtime_before_owned(deadline, cancellation)
                .await?
            {
                return Err(ServiceError::Unavailable);
            }
        }

        // A provider switch cannot use paper restart authority admitted for another provider or
        // another Direct session. Retire that incompatible request only after the old runtime.
        let retained_request = self.compatible_restart_request_owned(expected).await?;
        match retained_request {
            Some(request) => {
                self.controller
                    .start_paper_before_owned(&request, deadline, cancellation)
                    .await?;
            }
            None => match expected {
                PaperProvider::Public(provider) => {
                    self.controller
                        .start_public_source_owned(provider, deadline, cancellation)
                        .await?;
                }
                PaperProvider::CoinbaseDirect { .. } => return Err(ServiceError::NotFound),
            },
        }
        self.mark_selected_source_owned(expected, deadline, cancellation)
            .await?;
        wait_for_evidence(self, provider, deadline, cancellation).await
    }

    /// Tears down and restarts one generation, proving that durable source generation advanced.
    pub(crate) async fn resynchronize(
        &self,
        provider: &SourceIdentifier,
        expected_generation: ConnectionGeneration,
        onboarding_session_id: Option<uuid::Uuid>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(ConnectionGeneration, PaperSourceLifecycleEvidence), ServiceError> {
        let _owner =
            super::bounded_lock(&self.controller.owner_gate, deadline, cancellation).await?;
        let previous = self
            .stop_owned(
                provider,
                Some(expected_generation),
                false,
                deadline,
                cancellation,
            )
            .await?
            .ok_or(ServiceError::Unavailable)?;
        let current = self
            .start_owned(provider, onboarding_session_id, deadline, cancellation)
            .await?;
        if current.generation.get() <= previous.get() {
            return Err(ServiceError::Unavailable);
        }
        Ok((previous, current))
    }

    /// Stops runtime authority and discards the retained restart request.
    pub(crate) async fn remove(
        &self,
        provider: &SourceIdentifier,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ConnectionGeneration>, ServiceError> {
        let _owner =
            super::bounded_lock(&self.controller.owner_gate, deadline, cancellation).await?;
        let current = self.current_provider_owned(deadline, cancellation).await?;
        let previous = if current.is_some_and(|current| provider_matches(provider, current)) {
            let restore_public_source = self
                .other_selected_public_source_owned(provider, deadline, cancellation)
                .await?;
            let previous = self
                .owned_generation(provider, deadline, cancellation)
                .await?;
            if !self
                .controller
                .stop_runtime_before_owned(deadline, cancellation)
                .await?
            {
                return Err(ServiceError::Unavailable);
            }
            self.clear_matching_restart_request_owned(provider).await?;
            if let Some(restore) = restore_public_source {
                let _compatible = self
                    .compatible_restart_request_owned(PaperProvider::Public(restore))
                    .await?;
                self.controller
                    .start_public_source_owned(restore, deadline, cancellation)
                    .await?;
            }
            previous
        } else {
            self.clear_selected_public_source_owned(provider, deadline, cancellation)
                .await?;
            self.clear_matching_restart_request_owned(provider).await?;
            None
        };
        Ok(previous)
    }

    async fn current_provider_owned(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<PaperProvider>, ServiceError> {
        let state = super::bounded_lock(&self.controller.state, deadline, cancellation).await?;
        match &*state {
            PaperState::Stopped { .. } => Ok(None),
            PaperState::LiveOnly { provider, .. } => Ok(Some(PaperProvider::Public(*provider))),
            PaperState::Running { provider, .. } => Ok(Some(*provider)),
            PaperState::LiveStarting { .. }
            | PaperState::Starting { .. }
            | PaperState::Stopping => Err(ServiceError::Unavailable),
        }
    }

    async fn compatible_restart_request_owned(
        &self,
        expected: PaperProvider,
    ) -> Result<Option<market_squawk_services::TypedToolRequest>, ServiceError> {
        let mut retained = self.controller.restart_request.lock().await;
        let Some(request) = retained.as_ref() else {
            return Ok(None);
        };
        if exact_provider(PaperProvider::from_request(request)?, expected) {
            Ok(Some(request.clone()))
        } else {
            *retained = None;
            Ok(None)
        }
    }

    async fn clear_matching_restart_request_owned(
        &self,
        provider: &SourceIdentifier,
    ) -> Result<(), ServiceError> {
        let mut retained = self.controller.restart_request.lock().await;
        if retained
            .as_ref()
            .map(PaperProvider::from_request)
            .transpose()?
            .is_some_and(|current| provider_matches(provider, current))
        {
            *retained = None;
        }
        Ok(())
    }

    async fn other_selected_public_source_owned(
        &self,
        retiring: &SourceIdentifier,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ProductionSourceProvider>, ServiceError> {
        let state = super::bounded_lock(&self.controller.state, deadline, cancellation).await?;
        match &*state {
            PaperState::Running {
                restore_public_source: Some(provider),
                ..
            } if !provider_matches(retiring, PaperProvider::Public(*provider)) => {
                Ok(Some(*provider))
            }
            PaperState::Stopped { .. }
            | PaperState::LiveOnly { .. }
            | PaperState::Running { .. } => Ok(None),
            PaperState::LiveStarting { .. }
            | PaperState::Starting { .. }
            | PaperState::Stopping => Err(ServiceError::Unavailable),
        }
    }

    async fn clear_selected_public_source_owned(
        &self,
        provider: &SourceIdentifier,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let mut state = super::bounded_lock(&self.controller.state, deadline, cancellation).await?;
        match &mut *state {
            PaperState::Running {
                restore_public_source,
                ..
            } => {
                if restore_public_source.as_ref().is_some_and(|current| {
                    provider_matches(provider, PaperProvider::Public(*current))
                }) {
                    *restore_public_source = None;
                }
                Ok(())
            }
            PaperState::Stopped { .. } | PaperState::LiveOnly { .. } => Ok(()),
            PaperState::LiveStarting { .. }
            | PaperState::Starting { .. }
            | PaperState::Stopping => Err(ServiceError::Unavailable),
        }
    }

    async fn mark_selected_source_owned(
        &self,
        expected: PaperProvider,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        let mut state = super::bounded_lock(&self.controller.state, deadline, cancellation).await?;
        match (expected, &mut *state) {
            (
                PaperProvider::Public(provider),
                PaperState::LiveOnly {
                    provider: current, ..
                },
            ) if *current == provider => Ok(()),
            (
                PaperProvider::Public(provider),
                PaperState::Running {
                    provider: PaperProvider::Public(current),
                    restore_public_source,
                    ..
                },
            ) if *current == provider => {
                *restore_public_source = Some(provider);
                Ok(())
            }
            (
                expected @ PaperProvider::CoinbaseDirect { .. },
                PaperState::Running {
                    provider: current,
                    restore_public_source,
                    ..
                },
            ) if exact_provider(expected, *current) => {
                *restore_public_source = None;
                Ok(())
            }
            (
                _,
                PaperState::LiveStarting { .. }
                | PaperState::Starting { .. }
                | PaperState::Stopping,
            ) => Err(ServiceError::Unavailable),
            (
                _,
                PaperState::Stopped { .. }
                | PaperState::LiveOnly { .. }
                | PaperState::Running { .. },
            ) => Err(ServiceError::InvalidRequest),
        }
    }

    async fn owned_generation(
        &self,
        provider: &SourceIdentifier,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ConnectionGeneration>, ServiceError> {
        let state = super::bounded_lock(&self.controller.state, deadline, cancellation).await?;
        match &*state {
            PaperState::Stopped { .. }
            | PaperState::LiveStarting { .. }
            | PaperState::Starting { .. }
            | PaperState::Stopping => {
                let request = self.controller.restart_request.lock().await;
                let retained = request
                    .as_ref()
                    .map(PaperProvider::from_request)
                    .transpose()?;
                if retained.is_some_and(|current| !provider_matches(provider, current)) {
                    return Err(ServiceError::InvalidRequest);
                }
                Ok(None)
            }
            PaperState::LiveOnly {
                provider: current,
                runtime,
                ..
            } => {
                if !provider_matches(provider, PaperProvider::Public(*current)) {
                    return Err(ServiceError::InvalidRequest);
                }
                aggregate_generation(runtime.snapshots())
            }
            PaperState::Running {
                provider: current,
                runtime,
                ..
            } => {
                if !provider_matches(provider, *current) {
                    return Err(ServiceError::InvalidRequest);
                }
                aggregate_generation(runtime.snapshots())
            }
        }
    }
}

fn provider_kind(
    provider: &SourceIdentifier,
    onboarding_session_id: Option<uuid::Uuid>,
) -> Result<PaperProvider, ServiceError> {
    match provider.as_str() {
        "coinbase.public-market-data" => {
            Ok(PaperProvider::Public(ProductionSourceProvider::Coinbase))
        }
        "kraken.spot-public-market-data" => {
            Ok(PaperProvider::Public(ProductionSourceProvider::Kraken))
        }
        "coinbase.exchange-direct-market-data" => onboarding_session_id
            .map(|provider_session_id| PaperProvider::CoinbaseDirect {
                provider_session_id,
            })
            .ok_or(ServiceError::InvalidRequest),
        _ => Err(ServiceError::NotFound),
    }
}

fn exact_provider(left: PaperProvider, right: PaperProvider) -> bool {
    match (left, right) {
        (PaperProvider::Public(left), PaperProvider::Public(right)) => left == right,
        (
            PaperProvider::CoinbaseDirect {
                provider_session_id: left,
            },
            PaperProvider::CoinbaseDirect {
                provider_session_id: right,
            },
        ) => left == right,
        (PaperProvider::Public(_), PaperProvider::CoinbaseDirect { .. })
        | (PaperProvider::CoinbaseDirect { .. }, PaperProvider::Public(_)) => false,
    }
}

fn provider_matches(provider: &SourceIdentifier, current: PaperProvider) -> bool {
    matches!(
        (provider.as_str(), current),
        (
            "coinbase.public-market-data",
            PaperProvider::Public(ProductionSourceProvider::Coinbase),
        ) | (
            "kraken.spot-public-market-data",
            PaperProvider::Public(ProductionSourceProvider::Kraken),
        ) | (
            "coinbase.exchange-direct-market-data",
            PaperProvider::CoinbaseDirect { .. },
        )
    )
}

fn aggregate_generation(
    reader: market_squawk_live::LiveSnapshotReader,
) -> Result<Option<ConnectionGeneration>, ServiceError> {
    let lease = reader
        .try_load_all()
        .map_err(|_error| ServiceError::Unavailable)?;
    let mut generation = None;
    for shard in lease.snapshots() {
        for route in shard.routes() {
            for stream in route.streams() {
                let candidate = stream.connection_generation();
                if generation.is_some_and(|current| current != candidate) {
                    return Err(ServiceError::Unavailable);
                }
                generation = Some(candidate);
            }
        }
    }
    Ok(generation)
}

fn aggregate(
    provider: SourceIdentifier,
    reader: market_squawk_live::LiveSnapshotReader,
) -> Result<Option<PaperSourceLifecycleEvidence>, ServiceError> {
    let lease = reader
        .try_load_all()
        .map_err(|_error| ServiceError::Unavailable)?;
    let mut aggregate = None;
    for shard in lease.snapshots() {
        for route in shard.routes() {
            for stream in route.streams() {
                let runtime = stream
                    .runtime_evidence()
                    .filter(|evidence| evidence.matches_stream(stream))
                    .ok_or(ServiceError::Unavailable)?;
                let candidate = PaperSourceLifecycleEvidence {
                    provider: provider.clone(),
                    generation: stream.connection_generation(),
                    coverage: runtime.coverage_status(),
                    integrity: runtime.stream_integrity(),
                    quality: runtime.quality(),
                    observed_at: runtime.health_observed_at(),
                };
                aggregate = Some(match aggregate {
                    None => candidate,
                    Some(previous) => merge(previous, candidate)?,
                });
            }
        }
    }
    Ok(aggregate)
}

fn merge(
    mut aggregate: PaperSourceLifecycleEvidence,
    candidate: PaperSourceLifecycleEvidence,
) -> Result<PaperSourceLifecycleEvidence, ServiceError> {
    if aggregate.provider != candidate.provider || aggregate.generation != candidate.generation {
        return Err(ServiceError::Unavailable);
    }
    aggregate.coverage = weakest_coverage(aggregate.coverage, candidate.coverage);
    aggregate.integrity = weakest_integrity(aggregate.integrity, candidate.integrity);
    aggregate.quality = weakest_quality(aggregate.quality, candidate.quality);
    aggregate.observed_at = aggregate.observed_at.min(candidate.observed_at);
    Ok(aggregate)
}

const fn weakest_coverage(left: CoverageStatus, right: CoverageStatus) -> CoverageStatus {
    match (left, right) {
        (CoverageStatus::Unknown, _) | (_, CoverageStatus::Unknown) => CoverageStatus::Unknown,
        (CoverageStatus::Insufficient, _) | (_, CoverageStatus::Insufficient) => {
            CoverageStatus::Insufficient
        }
        (CoverageStatus::Sufficient, CoverageStatus::Sufficient) => CoverageStatus::Sufficient,
    }
}

const fn weakest_integrity(
    left: StreamIntegrityState,
    right: StreamIntegrityState,
) -> StreamIntegrityState {
    if integrity_rank(left) >= integrity_rank(right) {
        left
    } else {
        right
    }
}

const fn integrity_rank(value: StreamIntegrityState) -> u8 {
    match value {
        StreamIntegrityState::Healthy => 0,
        StreamIntegrityState::Initializing => 1,
        StreamIntegrityState::Synchronizing => 2,
        StreamIntegrityState::Validating => 3,
        StreamIntegrityState::Stale => 4,
        StreamIntegrityState::GapDetected => 5,
        StreamIntegrityState::Divergent => 6,
        StreamIntegrityState::ChecksumFailed => 7,
        StreamIntegrityState::Quarantined => 8,
    }
}

const fn weakest_quality(left: DataQuality, right: DataQuality) -> DataQuality {
    if quality_rank(left) >= quality_rank(right) {
        left
    } else {
        right
    }
}

const fn quality_rank(value: DataQuality) -> u8 {
    match value {
        DataQuality::DirectVerified => 0,
        DataQuality::DirectUnverified => 1,
        DataQuality::OfficialDelayed => 2,
        DataQuality::Aggregated => 3,
        DataQuality::Indicative => 4,
        DataQuality::Modeled => 5,
        DataQuality::Estimated => 6,
        DataQuality::Stale => 7,
        DataQuality::Quarantined => 8,
    }
}

async fn wait_for_evidence(
    control: &PaperSourceLifecycleControl,
    provider: &SourceIdentifier,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<PaperSourceLifecycleEvidence, ServiceError> {
    loop {
        match control.verify(provider, deadline, cancellation).await {
            Ok(Some(evidence)) => return Ok(evidence),
            Ok(None) | Err(ServiceError::Unavailable) => {}
            Err(error) => return Err(error),
        }
        if Instant::now() >= deadline {
            return Err(ServiceError::DeadlineExceeded);
        }
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(ServiceError::Cancelled),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(ServiceError::DeadlineExceeded);
            }
            () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {}
        }
    }
}
