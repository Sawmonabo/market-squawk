//! Bounded post-decision export of committed qualified market observations.

use std::mem::size_of;
use std::num::NonZeroUsize;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::{CommittedQualifiedMarketObservation, ShardKey};

const MAX_EXPORT_OBSERVATIONS: usize = 65_536;
const MAX_EXPORT_RETAINED_BYTES: usize = 1024 * 1024 * 1024;
const CHANNEL_ALLOCATION_OVERHEAD_BYTES: usize = 4_096;

/// Invalid bounded export-channel configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum QualifiedMarketExportError {
    /// The count or retained-byte budget is zero or above its fixed process ceiling.
    #[error("qualified market export capacity is invalid")]
    InvalidCapacity,
    /// Checked startup-memory accounting overflowed.
    #[error("qualified market export memory accounting overflowed")]
    CapacityOverflow,
}

/// One routed, count-and-byte-bounded post-decision export sender.
///
/// Only the live runtime can send through this value. Callers receive the paired read-only
/// receiver and cannot construct or inject committed observations.
#[derive(Debug)]
pub struct RouteQualifiedMarketExport {
    route: ShardKey,
    sender: mpsc::Sender<QualifiedMarketObservationLease>,
    retained_budget: Arc<Semaphore>,
    reserved_bytes: NonZeroUsize,
}

impl RouteQualifiedMarketExport {
    /// Creates one route-owned bounded channel and its sole consumer.
    ///
    /// `maximum_retained_bytes` bounds the sum of conservative source-message charges held by
    /// queued or consumer-owned leases. Startup memory accounting includes this entire budget.
    ///
    /// # Errors
    ///
    /// Rejects zero/excessive capacities and checked accounting overflow.
    pub fn try_new(
        route: ShardKey,
        capacity: usize,
        maximum_retained_bytes: usize,
    ) -> Result<(Self, QualifiedMarketObservationReceiver), QualifiedMarketExportError> {
        if capacity == 0
            || capacity > MAX_EXPORT_OBSERVATIONS
            || maximum_retained_bytes == 0
            || maximum_retained_bytes > MAX_EXPORT_RETAINED_BYTES
        {
            return Err(QualifiedMarketExportError::InvalidCapacity);
        }
        let slot_bytes = capacity
            .checked_mul(size_of::<QualifiedMarketObservationLease>())
            .ok_or(QualifiedMarketExportError::CapacityOverflow)?;
        let reserved_bytes = maximum_retained_bytes
            .checked_add(slot_bytes)
            .and_then(|value| value.checked_add(CHANNEL_ALLOCATION_OVERHEAD_BYTES))
            .and_then(NonZeroUsize::new)
            .ok_or(QualifiedMarketExportError::CapacityOverflow)?;
        let (sender, receiver) = mpsc::channel(capacity);
        Ok((
            Self {
                route,
                sender,
                retained_budget: Arc::new(Semaphore::new(maximum_retained_bytes)),
                reserved_bytes,
            },
            QualifiedMarketObservationReceiver { receiver },
        ))
    }

    /// Returns the exact route whose post-decision observations this sender owns.
    pub const fn route(&self) -> &ShardKey {
        &self.route
    }

    /// Returns the complete memory reservation added to runtime startup accounting.
    pub const fn reserved_bytes(&self) -> NonZeroUsize {
        self.reserved_bytes
    }

    pub(crate) fn try_export(
        &self,
        observation: CommittedQualifiedMarketObservation,
        conservative_retained_bytes: u32,
    ) -> Result<(), QualifiedMarketExportDisposition> {
        let retained_budget = Arc::clone(&self.retained_budget)
            .try_acquire_many_owned(conservative_retained_bytes)
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => QualifiedMarketExportDisposition::Full,
                tokio::sync::TryAcquireError::Closed => QualifiedMarketExportDisposition::Closed,
            })?;
        self.sender
            .try_send(QualifiedMarketObservationLease {
                observation,
                _retained_budget: retained_budget,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => QualifiedMarketExportDisposition::Full,
                mpsc::error::TrySendError::Closed(_) => QualifiedMarketExportDisposition::Closed,
            })
    }
}

/// One consumer-owned observation and its retained-byte reservation.
#[derive(Debug)]
pub struct QualifiedMarketObservationLease {
    observation: CommittedQualifiedMarketObservation,
    _retained_budget: OwnedSemaphorePermit,
}

impl QualifiedMarketObservationLease {
    /// Returns the non-forgeable committed observation while retaining its memory reservation.
    pub const fn observation(&self) -> &CommittedQualifiedMarketObservation {
        &self.observation
    }

    /// Returns the conservative retained-byte charge transferred with this observation.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self._retained_budget.num_permits()
    }

    /// Transfers the genuine committed observation out of the export-channel byte lease.
    ///
    /// The channel reservation is released when this method consumes the lease. The receiving
    /// bounded authority must establish its own byte reservation before retaining the observation.
    #[must_use]
    pub fn into_observation(self) -> CommittedQualifiedMarketObservation {
        self.observation
    }
}

/// Sole consumer for one route's bounded post-decision exports.
#[derive(Debug)]
pub struct QualifiedMarketObservationReceiver {
    receiver: mpsc::Receiver<QualifiedMarketObservationLease>,
}

impl QualifiedMarketObservationReceiver {
    /// Waits for the next exported observation, or `None` after runtime sender shutdown.
    pub async fn recv(&mut self) -> Option<QualifiedMarketObservationLease> {
        self.receiver.recv().await
    }

    /// Takes the next exported observation without waiting.
    pub fn try_recv(
        &mut self,
    ) -> Result<QualifiedMarketObservationLease, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QualifiedMarketExportDisposition {
    Full,
    Closed,
}
