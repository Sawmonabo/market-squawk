//! Bounded single-writer cross-venue feature ownership with coalescing admission.

use std::mem::size_of;
use std::num::{NonZeroU64, NonZeroUsize};

use market_squawk_analytics::{
    CrossVenueFeatureError as KernelError, ExactFeatureRatio, ExpectedVenueSet, FeatureValidity,
    FeatureValue, VenueFeatureObservation, cross_venue_divergence,
};
use market_squawk_domain::{ConnectionGeneration, InstrumentId, Timestamp, VenueId};
use thiserror::Error;

#[path = "cross_venue/runtime.rs"]
mod runtime;

pub(crate) use runtime::{
    CrossVenuePlaneHandle, CrossVenueRoutePublisher, CrossVenueRuntimeReader,
    create_cross_venue_plane, runtime_command_bytes,
};

/// Compact exact update accepted by a cross-venue owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CrossVenueUpdate {
    instrument: InstrumentId,
    venue: VenueId,
    generation: ConnectionGeneration,
    midpoint: ExactFeatureRatio,
    observed_at: Timestamp,
}

impl CrossVenueUpdate {
    #[must_use]
    pub const fn new(
        instrument: InstrumentId,
        venue: VenueId,
        generation: ConnectionGeneration,
        midpoint: ExactFeatureRatio,
        observed_at: Timestamp,
    ) -> Self {
        Self {
            instrument,
            venue,
            generation,
            midpoint,
            observed_at,
        }
    }

    /// Returns the conservative retained command charge before coalescing.
    pub fn retained_bytes(&self) -> Result<usize, CrossVenueFeatureError> {
        size_of::<Self>()
            .checked_add(self.venue.retained_bytes())
            .ok_or(CrossVenueFeatureError::RetainedSizeOverflow)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingUpdate {
    generation: ConnectionGeneration,
    midpoint: ExactFeatureRatio,
    observed_at: Timestamp,
}

#[derive(Debug)]
struct VenueState {
    venue: VenueId,
    generation: Option<ConnectionGeneration>,
    midpoint: Option<ExactFeatureRatio>,
    observed_at: Option<Timestamp>,
    pending: Option<PendingUpdate>,
    refreshed_after_failure: bool,
}

#[derive(Debug)]
struct InstrumentState {
    instrument: InstrumentId,
    venues: Box<[VenueState]>,
    saturated: bool,
    cycle_failed: bool,
    runtime_admission_epoch: Option<u64>,
}

/// Deterministic single-writer owner for complete cross-venue observations.
#[derive(Debug)]
pub struct CrossVenueFeatureHub {
    instruments: Vec<InstrumentState>,
    maximum_instruments: NonZeroUsize,
    maximum_venues: NonZeroUsize,
    maximum_pending_commands: NonZeroUsize,
    maximum_pending_bytes: NonZeroUsize,
    maximum_age_nanos: NonZeroU64,
    pending_commands: usize,
    pending_bytes: usize,
}

impl CrossVenueFeatureHub {
    /// Preallocates bounded instrument and command ownership for one writer.
    pub fn try_new(
        maximum_instruments: NonZeroUsize,
        maximum_venues: NonZeroUsize,
        maximum_pending_commands: NonZeroUsize,
        maximum_pending_bytes: NonZeroUsize,
        maximum_age_nanos: NonZeroU64,
    ) -> Result<Self, CrossVenueFeatureError> {
        if maximum_venues.get() < 2
            || maximum_venues.get() > market_squawk_analytics::MAX_CROSS_VENUE_OBSERVATIONS
        {
            return Err(CrossVenueFeatureError::VenueCapacityInvalid);
        }
        let mut instruments = Vec::new();
        instruments
            .try_reserve_exact(maximum_instruments.get())
            .map_err(|_| CrossVenueFeatureError::Allocation)?;
        Ok(Self {
            instruments,
            maximum_instruments,
            maximum_venues,
            maximum_pending_commands,
            maximum_pending_bytes,
            maximum_age_nanos,
            pending_commands: 0,
            pending_bytes: 0,
        })
    }

    /// Registers a complete expected venue set before live publication.
    pub fn try_register(
        &mut self,
        instrument: InstrumentId,
        venues: &[VenueId],
    ) -> Result<(), CrossVenueFeatureError> {
        if self
            .instruments
            .iter()
            .any(|state| state.instrument == instrument)
        {
            return Err(CrossVenueFeatureError::DuplicateInstrument);
        }
        if self.instruments.len() == self.maximum_instruments.get() {
            return Err(CrossVenueFeatureError::InstrumentCapacityFull);
        }
        if venues.len() < 2 || venues.len() > self.maximum_venues.get() {
            return Err(CrossVenueFeatureError::VenueCapacityInvalid);
        }
        let mut ordered = venues.to_vec();
        ordered.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        if ordered.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(CrossVenueFeatureError::DuplicateVenue);
        }
        let venues = ordered
            .into_iter()
            .map(|venue| VenueState {
                venue,
                generation: None,
                midpoint: None,
                observed_at: None,
                pending: None,
                refreshed_after_failure: false,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        self.instruments.push(InstrumentState {
            instrument,
            venues,
            saturated: false,
            cycle_failed: false,
            runtime_admission_epoch: None,
        });
        self.instruments.sort_by_key(|state| state.instrument);
        Ok(())
    }

    /// Nonblocking coalescing admission. One pending slot is retained per instrument/venue.
    pub fn try_publish(&mut self, update: CrossVenueUpdate) -> Result<(), CrossVenueFeatureError> {
        let charge = size_of::<PendingUpdate>();
        let instrument = self
            .instruments
            .iter_mut()
            .find(|state| state.instrument == update.instrument)
            .ok_or(CrossVenueFeatureError::UnknownInstrument)?;
        let Some(venue_index) = instrument
            .venues
            .iter()
            .position(|state| state.venue == update.venue)
        else {
            mark_failed(instrument);
            return Err(CrossVenueFeatureError::UnexpectedVenue);
        };
        if let Err(error) = validate_progression(&instrument.venues[venue_index], &update) {
            mark_failed(instrument);
            return Err(error);
        }
        let venue = &mut instrument.venues[venue_index];
        if venue.pending.is_none() {
            let next_commands = self
                .pending_commands
                .checked_add(1)
                .ok_or(CrossVenueFeatureError::RetainedSizeOverflow)?;
            let next_bytes = self
                .pending_bytes
                .checked_add(charge)
                .ok_or(CrossVenueFeatureError::RetainedSizeOverflow)?;
            if next_commands > self.maximum_pending_commands.get()
                || next_bytes > self.maximum_pending_bytes.get()
            {
                mark_failed(instrument);
                return Err(CrossVenueFeatureError::CommandCapacityFull);
            }
            self.pending_commands = next_commands;
            self.pending_bytes = next_bytes;
        }
        venue.pending = Some(PendingUpdate {
            generation: update.generation,
            midpoint: update.midpoint,
            observed_at: update.observed_at,
        });
        Ok(())
    }

    /// Applies every coalesced slot in deterministic instrument/venue order.
    pub fn drain(&mut self) -> Result<usize, CrossVenueFeatureError> {
        let mut applied = 0_usize;
        for instrument in &mut self.instruments {
            for venue in &mut instrument.venues {
                let Some(pending) = venue.pending.take() else {
                    continue;
                };
                venue.generation = Some(pending.generation);
                venue.midpoint = Some(pending.midpoint);
                venue.observed_at = Some(pending.observed_at);
                if instrument.saturated {
                    venue.refreshed_after_failure = true;
                }
                applied = applied
                    .checked_add(1)
                    .ok_or(CrossVenueFeatureError::RetainedSizeOverflow)?;
            }
            if instrument.saturated
                && !instrument.cycle_failed
                && instrument
                    .venues
                    .iter()
                    .all(|venue| venue.refreshed_after_failure)
            {
                instrument.saturated = false;
                for venue in &mut instrument.venues {
                    venue.refreshed_after_failure = false;
                }
            }
            instrument.cycle_failed = false;
        }
        self.pending_commands = 0;
        self.pending_bytes = 0;
        Ok(applied)
    }

    /// Builds one immutable complete-set snapshot without exposing mutable owner state.
    pub fn snapshot(
        &self,
        instrument: InstrumentId,
        evaluated_at: Timestamp,
    ) -> Result<CrossVenueFeatureSnapshot, CrossVenueFeatureError> {
        let state = self
            .instruments
            .iter()
            .find(|state| state.instrument == instrument)
            .ok_or(CrossVenueFeatureError::UnknownInstrument)?;
        let expected = state
            .venues
            .iter()
            .map(|venue| &venue.venue)
            .collect::<Vec<_>>();
        let expected = ExpectedVenueSet::try_new(&expected, self.maximum_venues)?;
        let observations = state
            .venues
            .iter()
            .filter_map(|venue| {
                venue
                    .midpoint
                    .zip(venue.observed_at)
                    .map(|(midpoint, observed_at)| {
                        VenueFeatureObservation::new(&venue.venue, midpoint, observed_at)
                    })
            })
            .collect::<Vec<_>>();
        let value = if state.saturated {
            FeatureValue::invalid(FeatureValidity::Unavailable, evaluated_at)?
        } else {
            cross_venue_divergence(
                &observations,
                expected,
                self.maximum_age_nanos,
                evaluated_at,
            )?
        };
        let venues = state
            .venues
            .iter()
            .map(|venue| CrossVenueVenueSnapshot {
                venue: venue.venue.clone(),
                generation: venue.generation,
                observed_at: venue.observed_at,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(CrossVenueFeatureSnapshot {
            instrument,
            venues,
            value,
        })
    }

    fn begin_runtime_admission_epoch(
        &mut self,
        instrument: InstrumentId,
        epoch: u64,
    ) -> Result<(), CrossVenueFeatureError> {
        let index = self
            .instruments
            .iter()
            .position(|state| state.instrument == instrument)
            .ok_or(CrossVenueFeatureError::UnknownInstrument)?;
        let instrument = &mut self.instruments[index];
        let removed = match instrument.runtime_admission_epoch {
            None => {
                instrument.runtime_admission_epoch = Some(epoch);
                0
            }
            Some(current) if current == epoch => 0,
            Some(current) if current < epoch => {
                instrument.runtime_admission_epoch = Some(epoch);
                instrument.saturated = true;
                instrument.cycle_failed = false;
                let mut removed = 0_usize;
                for venue in &mut instrument.venues {
                    if venue.pending.take().is_some() {
                        removed = removed
                            .checked_add(1)
                            .ok_or(CrossVenueFeatureError::RetainedSizeOverflow)?;
                    }
                    venue.refreshed_after_failure = false;
                }
                removed
            }
            Some(_) => return Err(CrossVenueFeatureError::AdmissionEpochRegression),
        };
        self.pending_commands = self
            .pending_commands
            .checked_sub(removed)
            .ok_or(CrossVenueFeatureError::RetainedSizeOverflow)?;
        self.pending_bytes = self
            .pending_bytes
            .checked_sub(
                removed
                    .checked_mul(size_of::<PendingUpdate>())
                    .ok_or(CrossVenueFeatureError::RetainedSizeOverflow)?,
            )
            .ok_or(CrossVenueFeatureError::RetainedSizeOverflow)?;
        Ok(())
    }

    fn runtime_admission_epoch(
        &self,
        instrument: InstrumentId,
    ) -> Result<Option<u64>, CrossVenueFeatureError> {
        self.instruments
            .iter()
            .find(|state| state.instrument == instrument)
            .map(|state| state.runtime_admission_epoch)
            .ok_or(CrossVenueFeatureError::UnknownInstrument)
    }

    #[must_use]
    pub const fn pending_command_count(&self) -> usize {
        self.pending_commands
    }
}

fn mark_failed(instrument: &mut InstrumentState) {
    instrument.saturated = true;
    instrument.cycle_failed = true;
    for venue in &mut instrument.venues {
        venue.refreshed_after_failure = false;
    }
}

fn validate_progression(
    state: &VenueState,
    update: &CrossVenueUpdate,
) -> Result<(), CrossVenueFeatureError> {
    let current_generation = state
        .pending
        .map_or(state.generation, |pending| Some(pending.generation));
    if current_generation.is_some_and(|current| update.generation.get() < current.get()) {
        return Err(CrossVenueFeatureError::GenerationRegression);
    }
    let current_time = state
        .pending
        .map_or(state.observed_at, |pending| Some(pending.observed_at));
    if current_generation == Some(update.generation)
        && current_time.is_some_and(|current| update.observed_at < current)
    {
        return Err(CrossVenueFeatureError::TimestampRegression);
    }
    Ok(())
}

/// Immutable venue participation metadata in canonical venue order.
#[derive(Debug, Eq, PartialEq)]
pub struct CrossVenueVenueSnapshot {
    venue: VenueId,
    generation: Option<ConnectionGeneration>,
    observed_at: Option<Timestamp>,
}

impl CrossVenueVenueSnapshot {
    pub const fn venue(&self) -> &VenueId {
        &self.venue
    }

    pub const fn generation(&self) -> Option<ConnectionGeneration> {
        self.generation
    }

    pub const fn observed_at(&self) -> Option<Timestamp> {
        self.observed_at
    }
}

/// Immutable exact result over the complete configured venue set.
#[derive(Debug, Eq, PartialEq)]
pub struct CrossVenueFeatureSnapshot {
    instrument: InstrumentId,
    venues: Box<[CrossVenueVenueSnapshot]>,
    value: FeatureValue<ExactFeatureRatio>,
}

impl CrossVenueFeatureSnapshot {
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    pub fn venues(&self) -> &[CrossVenueVenueSnapshot] {
        &self.venues
    }

    pub const fn validity(&self) -> FeatureValidity {
        self.value.validity()
    }

    pub fn divergence(&self) -> Option<ExactFeatureRatio> {
        self.value.ready_value()
    }
}

/// Cross-venue ownership, capacity, progression, or kernel failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CrossVenueFeatureError {
    #[error("cross-venue capacities must be nonzero")]
    ZeroCapacity,
    #[error("cross-venue venue capacity must be between two and the production maximum")]
    VenueCapacityInvalid,
    #[error("cross-venue instrument capacity is full")]
    InstrumentCapacityFull,
    #[error("cross-venue command count or byte capacity is full")]
    CommandCapacityFull,
    #[error("cross-venue instrument is already registered")]
    DuplicateInstrument,
    #[error("cross-venue expected venue is duplicated")]
    DuplicateVenue,
    #[error("cross-venue instrument is not registered")]
    UnknownInstrument,
    #[error("cross-venue venue is not part of the expected set")]
    UnexpectedVenue,
    #[error("cross-venue connection generation moved backwards")]
    GenerationRegression,
    #[error("cross-venue observation timestamp moved backwards")]
    TimestampRegression,
    #[error("cross-venue runtime admission epoch moved backwards")]
    AdmissionEpochRegression,
    #[error("cross-venue retained-size accounting overflowed")]
    RetainedSizeOverflow,
    #[error("cross-venue preallocation failed")]
    Allocation,
    #[error(transparent)]
    Kernel(#[from] KernelError),
    #[error(transparent)]
    Feature(#[from] market_squawk_analytics::FeatureError),
}
