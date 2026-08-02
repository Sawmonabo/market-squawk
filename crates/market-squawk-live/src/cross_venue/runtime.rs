//! Supervised bounded cross-shard command and immutable publication plane.

use std::collections::HashMap;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arc_swap::ArcSwap;
use market_squawk_analytics::{ExactFeatureRatio, FeatureValidity, FeatureValue};
use market_squawk_domain::{ConnectionGeneration, InstrumentId, Timestamp, VenueId};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use super::{
    CrossVenueFeatureError, CrossVenueFeatureHub, CrossVenueFeatureSnapshot, CrossVenueUpdate,
};
use crate::runtime::LiveFeatureCapacity;
use crate::{LiveRouteConfig, ShardKey};

const COMMAND_ACCOUNTING_OVERHEAD: usize = 64;
const MAXIMUM_CROSS_VENUE_AGE_NANOS: u64 = 1_000_000_000;
const INITIAL_ADMISSION_EPOCH: u64 = 1;

#[derive(Debug)]
struct RuntimeCommand {
    route_slot: usize,
    generation: ConnectionGeneration,
    midpoint: ExactFeatureRatio,
    observed_at: Timestamp,
    admission_epoch: u64,
    _permit: OwnedSemaphorePermit,
}

pub(crate) const fn runtime_command_bytes() -> usize {
    size_of::<RuntimeCommand>() + COMMAND_ACCOUNTING_OVERHEAD
}

#[derive(Clone, Debug)]
pub(crate) struct CrossVenueRoutePublisher {
    route_slot: usize,
    instrument: InstrumentId,
    commands: mpsc::Sender<RuntimeCommand>,
    bytes: Arc<Semaphore>,
    saturated: Arc<AtomicBool>,
    admission_epoch: Arc<AtomicU64>,
    terminal: Arc<AtomicBool>,
}

impl CrossVenueRoutePublisher {
    pub(crate) fn try_publish(
        &self,
        generation: ConnectionGeneration,
        midpoint: ExactFeatureRatio,
        observed_at: Timestamp,
    ) -> Result<(), CrossVenueRuntimeError> {
        if self.terminal.load(Ordering::Acquire) {
            return Err(CrossVenueRuntimeError::AdmissionEpochExhausted);
        }
        let bytes = u32::try_from(runtime_command_bytes())
            .map_err(|_| CrossVenueRuntimeError::RetainedSizeOverflow)?;
        let permit = match Arc::clone(&self.bytes).try_acquire_many_owned(bytes) {
            Ok(permit) => permit,
            Err(error) => {
                self.record_admission_failure()?;
                return Err(match error {
                    tokio::sync::TryAcquireError::NoPermits => {
                        CrossVenueRuntimeError::ByteCapacityFull
                    }
                    tokio::sync::TryAcquireError::Closed => CrossVenueRuntimeError::Closed,
                });
            }
        };
        let admission_epoch = self.admission_epoch.load(Ordering::Acquire);
        if self.terminal.load(Ordering::Acquire) {
            return Err(CrossVenueRuntimeError::AdmissionEpochExhausted);
        }
        let command = RuntimeCommand {
            route_slot: self.route_slot,
            generation,
            midpoint,
            observed_at,
            admission_epoch,
            _permit: permit,
        };
        match self.commands.try_send(command) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.record_admission_failure()?;
                Err(match error {
                    mpsc::error::TrySendError::Full(_) => CrossVenueRuntimeError::CountCapacityFull,
                    mpsc::error::TrySendError::Closed(_) => CrossVenueRuntimeError::Closed,
                })
            }
        }
    }

    pub(crate) const fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    fn record_admission_failure(&self) -> Result<(), CrossVenueRuntimeError> {
        self.saturated.store(true, Ordering::Release);
        if self.terminal.load(Ordering::Acquire) {
            return Err(CrossVenueRuntimeError::AdmissionEpochExhausted);
        }
        if self
            .admission_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| {
                epoch.checked_add(1)
            })
            .is_err()
        {
            self.terminal.store(true, Ordering::Release);
            return Err(CrossVenueRuntimeError::AdmissionEpochExhausted);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct PublishedInstrument {
    snapshot: ArcSwap<CrossVenueFeatureSnapshot>,
    saturated: Arc<AtomicBool>,
    admission_epoch: Arc<AtomicU64>,
    published_epoch: AtomicU64,
    terminal: Arc<AtomicBool>,
}

#[derive(Clone, Debug)]
pub(crate) struct CrossVenueRuntimeReader {
    instruments: Arc<HashMap<InstrumentId, Arc<PublishedInstrument>>>,
}

impl CrossVenueRuntimeReader {
    pub(crate) fn load(
        &self,
        instrument: InstrumentId,
        observed_at: Timestamp,
    ) -> Result<FeatureValue<ExactFeatureRatio>, CrossVenueRuntimeError> {
        let Some(published) = self.instruments.get(&instrument) else {
            return Ok(FeatureValue::invalid(
                FeatureValidity::Unavailable,
                observed_at,
            )?);
        };
        let admission_epoch = published.admission_epoch.load(Ordering::Acquire);
        if published.terminal.load(Ordering::Acquire)
            || published.saturated.load(Ordering::Acquire)
            || published.published_epoch.load(Ordering::Acquire) != admission_epoch
        {
            return Ok(FeatureValue::invalid(
                FeatureValidity::Unavailable,
                observed_at,
            )?);
        }
        let snapshot = published.snapshot.load();
        if !snapshot.validity().is_ready() {
            return Ok(FeatureValue::invalid(snapshot.validity(), observed_at)?);
        }
        for venue in snapshot.venues() {
            let Some(venue_time) = venue.observed_at() else {
                return Ok(FeatureValue::invalid(
                    FeatureValidity::Unavailable,
                    observed_at,
                )?);
            };
            let age = i128::from(observed_at.unix_nanos()) - i128::from(venue_time.unix_nanos());
            if age < 0 {
                return Ok(FeatureValue::invalid(
                    FeatureValidity::TimestampRegression,
                    observed_at,
                )?);
            }
            if age > i128::from(MAXIMUM_CROSS_VENUE_AGE_NANOS) {
                return Ok(FeatureValue::invalid(FeatureValidity::Stale, observed_at)?);
            }
        }
        let value = snapshot
            .divergence()
            .ok_or(CrossVenueRuntimeError::PublicationInvariant)?;
        Ok(FeatureValue::ready(value, observed_at))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CrossVenuePlaneHandle {
    routes: Arc<HashMap<ShardKey, CrossVenueRoutePublisher>>,
    reader: CrossVenueRuntimeReader,
}

impl CrossVenuePlaneHandle {
    pub(crate) fn route(
        &self,
        route: &ShardKey,
    ) -> Option<(CrossVenueRoutePublisher, CrossVenueRuntimeReader)> {
        self.routes
            .get(route)
            .cloned()
            .map(|publisher| (publisher, self.reader.clone()))
    }
}

#[derive(Debug)]
struct RouteIdentity {
    instrument: InstrumentId,
    venue: VenueId,
}

#[derive(Debug)]
pub(crate) struct CrossVenueWorker {
    hub: CrossVenueFeatureHub,
    routes: Box<[RouteIdentity]>,
    commands: mpsc::Receiver<RuntimeCommand>,
    published: Arc<HashMap<InstrumentId, Arc<PublishedInstrument>>>,
    maximum_batch: NonZeroUsize,
    affected: Vec<(InstrumentId, Timestamp)>,
    failed: Vec<InstrumentId>,
    cancellation: CancellationToken,
}

impl CrossVenueWorker {
    pub(crate) async fn run(mut self) {
        loop {
            let first = tokio::select! {
                biased;
                () = self.cancellation.cancelled() => break,
                command = self.commands.recv() => command,
            };
            let Some(first) = first else {
                break;
            };
            let mut affected = std::mem::take(&mut self.affected);
            let mut failed = std::mem::take(&mut self.failed);
            affected.clear();
            failed.clear();
            self.accept(first, &mut affected, &mut failed);
            for _ in 1..self.maximum_batch.get() {
                let Ok(command) = self.commands.try_recv() else {
                    break;
                };
                self.accept(command, &mut affected, &mut failed);
            }
            if self.hub.drain().is_err() {
                mark_saturated(&self.published, &affected);
                self.affected = affected;
                self.failed = failed;
                continue;
            }
            affected.sort_unstable();
            affected.dedup();
            failed.sort_unstable();
            failed.dedup();
            for (instrument, evaluated_at) in &affected {
                let Some(cell) = self.published.get(instrument) else {
                    continue;
                };
                if failed.binary_search(instrument).is_ok() {
                    cell.saturated.store(true, Ordering::Release);
                    continue;
                }
                let admission_epoch = cell.admission_epoch.load(Ordering::Acquire);
                if cell.terminal.load(Ordering::Acquire) {
                    cell.saturated.store(true, Ordering::Release);
                    continue;
                }
                match self.hub.runtime_admission_epoch(*instrument) {
                    Ok(Some(hub_epoch)) if hub_epoch == admission_epoch => {}
                    Ok(_) => {
                        if self
                            .hub
                            .begin_runtime_admission_epoch(*instrument, admission_epoch)
                            .is_err()
                        {
                            cell.saturated.store(true, Ordering::Release);
                            continue;
                        }
                    }
                    Err(_) => {
                        cell.saturated.store(true, Ordering::Release);
                        continue;
                    }
                }
                match self.hub.snapshot(*instrument, *evaluated_at) {
                    Ok(snapshot) => {
                        if cell.terminal.load(Ordering::Acquire)
                            || cell.admission_epoch.load(Ordering::Acquire) != admission_epoch
                        {
                            cell.saturated.store(true, Ordering::Release);
                            continue;
                        }
                        cell.snapshot.store(Arc::new(snapshot));
                        cell.published_epoch
                            .store(admission_epoch, Ordering::Release);
                        cell.saturated.store(false, Ordering::Release);
                    }
                    Err(_) => cell.saturated.store(true, Ordering::Release),
                }
            }
            self.affected = affected;
            self.failed = failed;
        }
    }

    fn accept(
        &mut self,
        command: RuntimeCommand,
        affected: &mut Vec<(InstrumentId, Timestamp)>,
        failed: &mut Vec<InstrumentId>,
    ) {
        let Some(route) = self.routes.get(command.route_slot) else {
            return;
        };
        affected.push((route.instrument, command.observed_at));
        let Some(cell) = self.published.get(&route.instrument) else {
            failed.push(route.instrument);
            return;
        };
        if cell.terminal.load(Ordering::Acquire) {
            failed.push(route.instrument);
            return;
        }
        let admission_epoch = cell.admission_epoch.load(Ordering::Acquire);
        if self
            .hub
            .begin_runtime_admission_epoch(route.instrument, admission_epoch)
            .is_err()
        {
            failed.push(route.instrument);
            return;
        }
        if command.admission_epoch != admission_epoch {
            return;
        }
        let update = CrossVenueUpdate::new(
            route.instrument,
            route.venue.clone(),
            command.generation,
            command.midpoint,
            command.observed_at,
        );
        if self.hub.try_publish(update).is_err() {
            failed.push(route.instrument);
        }
    }
}

fn mark_saturated(
    published: &HashMap<InstrumentId, Arc<PublishedInstrument>>,
    affected: &[(InstrumentId, Timestamp)],
) {
    for (instrument, _) in affected {
        if let Some(cell) = published.get(instrument) {
            cell.saturated.store(true, Ordering::Release);
        }
    }
}

pub(crate) fn create_cross_venue_plane(
    routes: &[LiveRouteConfig],
    capacity: LiveFeatureCapacity,
    cancellation: CancellationToken,
) -> Result<(CrossVenuePlaneHandle, Option<CrossVenueWorker>), CrossVenueRuntimeError> {
    if usize::try_from(capacity.cross_venue_command_bytes.get())
        .map_err(|_| CrossVenueRuntimeError::RetainedSizeOverflow)?
        < runtime_command_bytes()
    {
        return Err(CrossVenueRuntimeError::CommandByteCapacityTooSmall);
    }
    let mut ordered_routes = Vec::new();
    ordered_routes
        .try_reserve_exact(routes.len())
        .map_err(|_| CrossVenueRuntimeError::Allocation)?;
    ordered_routes.extend(routes);
    ordered_routes.sort_unstable_by(|left, right| {
        left.route()
            .instrument()
            .cmp(&right.route().instrument())
            .then_with(|| {
                left.route()
                    .venue()
                    .as_str()
                    .cmp(right.route().venue().as_str())
            })
    });
    let cross_venue_instruments = validate_route_groups(&ordered_routes, capacity)?;
    let mut hub = CrossVenueFeatureHub::try_new(
        capacity.maximum_cross_venue_instruments,
        capacity.maximum_venues_per_cross_venue_instrument,
        capacity.cross_venue_command_count,
        NonZeroUsize::new(
            usize::try_from(capacity.cross_venue_command_bytes.get())
                .map_err(|_| CrossVenueRuntimeError::RetainedSizeOverflow)?,
        )
        .ok_or(CrossVenueRuntimeError::RetainedSizeOverflow)?,
        std::num::NonZeroU64::new(MAXIMUM_CROSS_VENUE_AGE_NANOS)
            .ok_or(CrossVenueRuntimeError::RetainedSizeOverflow)?,
    )?;
    let (commands, receiver) = mpsc::channel(capacity.cross_venue_command_count.get());
    let bytes = Arc::new(Semaphore::new(
        usize::try_from(capacity.cross_venue_command_bytes.get())
            .map_err(|_| CrossVenueRuntimeError::RetainedSizeOverflow)?,
    ));
    let mut route_identities = Vec::new();
    route_identities
        .try_reserve_exact(routes.len())
        .map_err(|_| CrossVenueRuntimeError::Allocation)?;
    let mut route_publishers = HashMap::new();
    route_publishers
        .try_reserve(routes.len())
        .map_err(|_| CrossVenueRuntimeError::Allocation)?;
    let mut published = HashMap::new();
    published
        .try_reserve(cross_venue_instruments)
        .map_err(|_| CrossVenueRuntimeError::Allocation)?;
    let mut group_start = 0;
    while group_start < ordered_routes.len() {
        let group_end = instrument_group_end(&ordered_routes, group_start);
        let expected = &ordered_routes[group_start..group_end];
        group_start = group_end;
        if expected.len() < 2 {
            continue;
        }
        let instrument = expected[0].route().instrument();
        let mut venues = Vec::new();
        venues
            .try_reserve_exact(expected.len())
            .map_err(|_| CrossVenueRuntimeError::Allocation)?;
        venues.extend(expected.iter().map(|route| route.route().venue().clone()));
        hub.try_register(instrument, &venues)?;
        let initial = hub.snapshot(instrument, Timestamp::from_unix_nanos(0))?;
        let saturated = Arc::new(AtomicBool::new(false));
        let admission_epoch = Arc::new(AtomicU64::new(INITIAL_ADMISSION_EPOCH));
        let terminal = Arc::new(AtomicBool::new(false));
        let cell = Arc::new(PublishedInstrument {
            snapshot: ArcSwap::from_pointee(initial),
            saturated: Arc::clone(&saturated),
            admission_epoch: Arc::clone(&admission_epoch),
            published_epoch: AtomicU64::new(0),
            terminal: Arc::clone(&terminal),
        });
        if published.insert(instrument, cell).is_some() {
            return Err(CrossVenueRuntimeError::DuplicateInstrument);
        }
        for route in expected {
            let route_slot = route_identities.len();
            route_identities.push(RouteIdentity {
                instrument,
                venue: route.route().venue().clone(),
            });
            if route_publishers
                .insert(
                    route.route().clone(),
                    CrossVenueRoutePublisher {
                        route_slot,
                        instrument,
                        commands: commands.clone(),
                        bytes: Arc::clone(&bytes),
                        saturated: Arc::clone(&saturated),
                        admission_epoch: Arc::clone(&admission_epoch),
                        terminal: Arc::clone(&terminal),
                    },
                )
                .is_some()
            {
                return Err(CrossVenueRuntimeError::DuplicateRoute);
            }
        }
    }
    let published = Arc::new(published);
    let mut affected = Vec::new();
    affected
        .try_reserve_exact(capacity.cross_venue_command_count.get())
        .map_err(|_| CrossVenueRuntimeError::Allocation)?;
    let mut failed = Vec::new();
    failed
        .try_reserve_exact(capacity.cross_venue_command_count.get())
        .map_err(|_| CrossVenueRuntimeError::Allocation)?;
    let reader = CrossVenueRuntimeReader {
        instruments: Arc::clone(&published),
    };
    let handle = CrossVenuePlaneHandle {
        routes: Arc::new(route_publishers),
        reader,
    };
    let worker = (cross_venue_instruments != 0).then_some(CrossVenueWorker {
        hub,
        routes: route_identities.into_boxed_slice(),
        commands: receiver,
        published,
        maximum_batch: capacity.cross_venue_command_count,
        affected,
        failed,
        cancellation,
    });
    Ok((handle, worker))
}

fn validate_route_groups(
    ordered_routes: &[&LiveRouteConfig],
    capacity: LiveFeatureCapacity,
) -> Result<usize, CrossVenueRuntimeError> {
    let mut instrument_count = 0_usize;
    let mut group_start = 0;
    while group_start < ordered_routes.len() {
        let group_end = instrument_group_end(ordered_routes, group_start);
        let venue_count = group_end - group_start;
        group_start = group_end;
        if venue_count < 2 {
            continue;
        }
        if venue_count > capacity.maximum_venues_per_cross_venue_instrument.get() {
            return Err(CrossVenueRuntimeError::VenueCapacityFull);
        }
        instrument_count = instrument_count
            .checked_add(1)
            .ok_or(CrossVenueRuntimeError::RetainedSizeOverflow)?;
        if instrument_count > capacity.maximum_cross_venue_instruments.get() {
            return Err(CrossVenueRuntimeError::InstrumentCapacityFull);
        }
    }
    Ok(instrument_count)
}

fn instrument_group_end(routes: &[&LiveRouteConfig], start: usize) -> usize {
    let instrument = routes[start].route().instrument();
    routes[start..]
        .iter()
        .position(|route| route.route().instrument() != instrument)
        .map_or(routes.len(), |offset| start + offset)
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum CrossVenueRuntimeError {
    #[error("cross-venue command byte capacity cannot hold one command")]
    CommandByteCapacityTooSmall,
    #[error("cross-venue command count capacity is full")]
    CountCapacityFull,
    #[error("cross-venue command byte capacity is full")]
    ByteCapacityFull,
    #[error("cross-venue instrument capacity is full")]
    InstrumentCapacityFull,
    #[error("cross-venue plane contains a duplicate instrument")]
    DuplicateInstrument,
    #[error("cross-venue plane contains a duplicate route")]
    DuplicateRoute,
    #[error("cross-venue venue capacity is full")]
    VenueCapacityFull,
    #[error("cross-venue command plane is closed")]
    Closed,
    #[error("cross-venue retained-size accounting overflowed")]
    RetainedSizeOverflow,
    #[error("cross-venue bounded allocation failed")]
    Allocation,
    #[error("cross-venue immutable publication violated its ready-value invariant")]
    PublicationInvariant,
    #[error("cross-venue admission epoch exhausted and the instrument is terminal")]
    AdmissionEpochExhausted,
    #[error(transparent)]
    Feature(#[from] CrossVenueFeatureError),
    #[error(transparent)]
    Value(#[from] market_squawk_analytics::FeatureError),
}

#[cfg(test)]
#[path = "runtime/tests.rs"]
mod tests;
