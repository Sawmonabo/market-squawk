//! Bounded, generation-aware application event fan-out.

use std::{
    collections::VecDeque,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use market_squawk_domain::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::{
    ClientId, EventCursor, EventCursorError, EventPageLimit, RuntimeContractError,
    ServiceGeneration,
};

/// Hard retention and encoded-size ceilings for one event hub.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventHubLimits {
    retained_events: NonZeroUsize,
    maximum_event_bytes: NonZeroUsize,
}

impl EventHubLimits {
    /// Creates positive fixed event bounds.
    pub fn try_new(
        retained_events: usize,
        maximum_event_bytes: usize,
    ) -> Result<Self, EventReadError> {
        Ok(Self {
            retained_events: NonZeroUsize::new(retained_events)
                .ok_or(EventReadError::InvalidLimits)?,
            maximum_event_bytes: NonZeroUsize::new(maximum_event_bytes)
                .ok_or(EventReadError::InvalidLimits)?,
        })
    }
}

/// Sequenced application event from one exact service generation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ApplicationEvent {
    generation: ServiceGeneration,
    sequence: u64,
    payload: Value,
}

impl ApplicationEvent {
    /// Exact service generation that produced this event.
    #[must_use]
    pub const fn generation(&self) -> ServiceGeneration {
        self.generation
    }

    /// Monotonic sequence within the service generation.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Closed event payload.
    #[must_use]
    pub const fn payload(&self) -> &Value {
        &self.payload
    }
}

/// One bounded event page and its next reconnect cursor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EventPage {
    events: Arc<[Arc<ApplicationEvent>]>,
    cursor: EventCursor,
}

impl EventPage {
    /// Events ordered by increasing sequence.
    #[must_use]
    pub const fn events(&self) -> &Arc<[Arc<ApplicationEvent>]> {
        &self.events
    }

    /// Cursor to present on the next read.
    #[must_use]
    pub const fn cursor(&self) -> &EventCursor {
        &self.cursor
    }
}

/// Non-blocking event hub whose producers never wait for clients.
#[derive(Debug)]
pub struct EventHub {
    generation: ServiceGeneration,
    limits: EventHubLimits,
    state: Mutex<EventState>,
}

#[derive(Debug)]
struct EventState {
    next_sequence: u64,
    retained: VecDeque<Arc<ApplicationEvent>>,
}

impl EventHub {
    /// Creates an empty event hub for one service generation.
    pub fn try_new(
        generation: ServiceGeneration,
        limits: EventHubLimits,
    ) -> Result<Self, EventReadError> {
        let retained = VecDeque::with_capacity(limits.retained_events.get());
        Ok(Self {
            generation,
            limits,
            state: Mutex::new(EventState {
                next_sequence: 1,
                retained,
            }),
        })
    }

    /// Publishes one bounded event without waiting for any client.
    pub fn publish(&self, payload: Value) -> Result<u64, EventReadError> {
        let encoded = serde_json::to_vec(&payload).map_err(|_| EventReadError::InvalidEvent)?;
        if encoded.len() > self.limits.maximum_event_bytes.get() {
            return Err(EventReadError::InvalidEvent);
        }
        let mut state = self.state.lock().map_err(|_| EventReadError::Unavailable)?;
        let sequence = state.next_sequence;
        state.next_sequence = sequence
            .checked_add(1)
            .ok_or(EventReadError::SequenceExhausted)?;
        if state.retained.len() == self.limits.retained_events.get() {
            state.retained.pop_front();
        }
        state.retained.push_back(Arc::new(ApplicationEvent {
            generation: self.generation,
            sequence,
            payload,
        }));
        Ok(sequence)
    }

    /// Returns a bounded page or requires snapshot resynchronization after any gap.
    pub fn read_after(
        &self,
        client_id: ClientId,
        cursor: Option<&EventCursor>,
        limit: EventPageLimit,
        now: Timestamp,
        cursor_expires_at: Timestamp,
    ) -> Result<EventPage, EventReadError> {
        if cursor_expires_at <= now {
            return Err(EventReadError::Cursor(EventCursorError::Expired));
        }
        if let Some(cursor) = cursor {
            cursor
                .ensure_current(client_id, self.generation, now)
                .map_err(EventReadError::Cursor)?;
        }
        let state = self.state.lock().map_err(|_| EventReadError::Unavailable)?;
        let requested_sequence = cursor.map_or(0, EventCursor::sequence);
        let oldest_available = state
            .retained
            .front()
            .map_or(state.next_sequence, |event| event.sequence());
        if requested_sequence.saturating_add(1) < oldest_available {
            return Err(EventReadError::SequenceGap { oldest_available });
        }
        let events: Arc<[Arc<ApplicationEvent>]> = state
            .retained
            .iter()
            .filter(|event| event.sequence > requested_sequence)
            .take(limit.get())
            .cloned()
            .collect::<Vec<_>>()
            .into();
        let next_sequence = events
            .last()
            .map_or(requested_sequence, |event| event.sequence());
        drop(state);
        let next =
            EventCursor::try_new(client_id, self.generation, next_sequence, cursor_expires_at)
                .map_err(EventReadError::Contract)?;
        Ok(EventPage {
            events,
            cursor: next,
        })
    }
}

/// Bounded event publication or continuation failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum EventReadError {
    /// Event bounds must be positive.
    #[error("event hub limits are invalid")]
    InvalidLimits,
    /// Event payload was not encodable within its fixed ceiling.
    #[error("application event is invalid")]
    InvalidEvent,
    /// The sequence counter cannot advance.
    #[error("application event sequence is exhausted")]
    SequenceExhausted,
    /// The cursor can no longer produce a contiguous projection.
    #[error(
        "event sequence gap begins at {oldest_available}; snapshot resynchronization is required"
    )]
    SequenceGap {
        /// Oldest event still retained.
        oldest_available: u64,
    },
    /// Cursor generation or expiry requires resynchronization.
    #[error(transparent)]
    Cursor(EventCursorError),
    /// Cursor construction failed.
    #[error(transparent)]
    Contract(RuntimeContractError),
    /// Event-state serialization is unavailable.
    #[error("application event hub is unavailable")]
    Unavailable,
}
