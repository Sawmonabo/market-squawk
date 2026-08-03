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
            PaperState::Starting { .. } | PaperState::Stopping => Err(ServiceError::Unavailable),
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
            PaperState::Running { .. } => Err(ServiceError::Unavailable),
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
            PaperState::Starting { .. } | PaperState::Stopping => Err(ServiceError::Unavailable),
            PaperState::Running {
                provider: current,
                runtime,
                exports,
                cancellation: runtime_cancellation,
            } => {
                if !provider_matches(provider, *current)
                    || runtime_cancellation.is_cancelled()
                    || !runtime.source_is_healthy()
                    || !exports.is_healthy()
                {
                    return Err(ServiceError::Unavailable);
                }
                aggregate(provider.clone(), runtime.snapshots())
            }
        }
    }

    /// Stops the exact live owner while retaining its previously admitted start request.
    pub(crate) async fn stop(
        &self,
        provider: &SourceIdentifier,
        expected_generation: Option<ConnectionGeneration>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ConnectionGeneration>, ServiceError> {
        let previous = self
            .owned_generation(provider, deadline, cancellation)
            .await?;
        if expected_generation.is_some() && previous != expected_generation {
            return Err(ServiceError::InvalidRequest);
        }
        if !self.controller.stop_before(deadline, cancellation).await? {
            return Err(ServiceError::Unavailable);
        }
        Ok(previous)
    }

    /// Restarts the exact request previously admitted through `Bot.Start`.
    pub(crate) async fn start(
        &self,
        provider: &SourceIdentifier,
        onboarding_session_id: Option<uuid::Uuid>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PaperSourceLifecycleEvidence, ServiceError> {
        if let Some(current) = self.verify(provider, deadline, cancellation).await? {
            return Ok(current);
        }
        let request = self
            .controller
            .restart_request
            .lock()
            .await
            .clone()
            .ok_or(ServiceError::NotFound)?;
        let retained = PaperProvider::from_request(&request)?;
        let expected = provider_kind(provider, onboarding_session_id)?;
        if !same_surface(retained, expected)
            || matches!(
                (retained, onboarding_session_id),
                (
                    PaperProvider::CoinbaseDirect {
                        provider_session_id: retained,
                    },
                    Some(requested),
                ) if retained != requested
            )
        {
            return Err(ServiceError::InvalidRequest);
        }
        self.controller
            .start_before(&request, deadline, cancellation)
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
        let previous = self
            .stop(provider, Some(expected_generation), deadline, cancellation)
            .await?
            .ok_or(ServiceError::Unavailable)?;
        let current = self
            .start(provider, onboarding_session_id, deadline, cancellation)
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
        let previous = self
            .owned_generation(provider, deadline, cancellation)
            .await?;
        if !self.controller.stop_before(deadline, cancellation).await? {
            return Err(ServiceError::Unavailable);
        }
        *self.controller.restart_request.lock().await = None;
        Ok(previous)
    }

    async fn owned_generation(
        &self,
        provider: &SourceIdentifier,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ConnectionGeneration>, ServiceError> {
        let state = super::bounded_lock(&self.controller.state, deadline, cancellation).await?;
        match &*state {
            PaperState::Stopped { .. } | PaperState::Starting { .. } | PaperState::Stopping => {
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

const fn same_surface(left: PaperProvider, right: PaperProvider) -> bool {
    matches!(
        (left, right),
        (
            PaperProvider::Public(ProductionSourceProvider::Coinbase),
            PaperProvider::Public(ProductionSourceProvider::Coinbase),
        ) | (
            PaperProvider::Public(ProductionSourceProvider::Kraken),
            PaperProvider::Public(ProductionSourceProvider::Kraken),
        ) | (
            PaperProvider::CoinbaseDirect { .. },
            PaperProvider::CoinbaseDirect { .. },
        )
    )
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
