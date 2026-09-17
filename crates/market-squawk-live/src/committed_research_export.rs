//! Bounded export of non-executable committed market observations for durable research.

use std::mem::size_of;
use std::num::NonZeroUsize;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::{CommittedResearchMarketObservation, ShardKey};

const MAX_EXPORT_OBSERVATIONS: usize = 65_536;
const MAX_EXPORT_RETAINED_BYTES: usize = 1024 * 1024 * 1024;
const CHANNEL_ALLOCATION_OVERHEAD_BYTES: usize = 4_096;

/// Invalid bounded research-export configuration.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CommittedResearchMarketExportError {
    #[error("committed research-market export capacity is invalid")]
    InvalidCapacity,
    #[error("committed research-market export memory accounting overflowed")]
    CapacityOverflow,
}

/// Route-owned, count-and-byte-bounded sender for committed DirectUnverified observations.
#[derive(Debug)]
pub struct RouteCommittedResearchMarketExport {
    route: ShardKey,
    sender: mpsc::Sender<CommittedResearchMarketObservationLease>,
    retained_budget: Arc<Semaphore>,
    reserved_bytes: NonZeroUsize,
}

impl RouteCommittedResearchMarketExport {
    pub fn try_new(
        route: ShardKey,
        capacity: usize,
        maximum_retained_bytes: usize,
    ) -> Result<
        (Self, CommittedResearchMarketObservationReceiver),
        CommittedResearchMarketExportError,
    > {
        if capacity == 0
            || capacity > MAX_EXPORT_OBSERVATIONS
            || maximum_retained_bytes == 0
            || maximum_retained_bytes > MAX_EXPORT_RETAINED_BYTES
        {
            return Err(CommittedResearchMarketExportError::InvalidCapacity);
        }
        let slot_bytes = capacity
            .checked_mul(size_of::<CommittedResearchMarketObservationLease>())
            .ok_or(CommittedResearchMarketExportError::CapacityOverflow)?;
        let reserved_bytes = maximum_retained_bytes
            .checked_add(slot_bytes)
            .and_then(|value| value.checked_add(CHANNEL_ALLOCATION_OVERHEAD_BYTES))
            .and_then(NonZeroUsize::new)
            .ok_or(CommittedResearchMarketExportError::CapacityOverflow)?;
        let (sender, receiver) = mpsc::channel(capacity);
        Ok((
            Self {
                route,
                sender,
                retained_budget: Arc::new(Semaphore::new(maximum_retained_bytes)),
                reserved_bytes,
            },
            CommittedResearchMarketObservationReceiver { receiver },
        ))
    }

    pub const fn route(&self) -> &ShardKey {
        &self.route
    }

    pub const fn reserved_bytes(&self) -> NonZeroUsize {
        self.reserved_bytes
    }

    pub(crate) fn try_export(
        &self,
        observation: CommittedResearchMarketObservation,
        conservative_retained_bytes: u32,
    ) -> Result<(), CommittedResearchMarketExportDisposition> {
        let retained_budget = Arc::clone(&self.retained_budget)
            .try_acquire_many_owned(conservative_retained_bytes)
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => {
                    CommittedResearchMarketExportDisposition::Full
                }
                tokio::sync::TryAcquireError::Closed => {
                    CommittedResearchMarketExportDisposition::Closed
                }
            })?;
        self.sender
            .try_send(CommittedResearchMarketObservationLease {
                observation,
                _retained_budget: retained_budget,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    CommittedResearchMarketExportDisposition::Full
                }
                mpsc::error::TrySendError::Closed(_) => {
                    CommittedResearchMarketExportDisposition::Closed
                }
            })
    }
}

/// One consumer-owned committed observation and its retained-byte reservation.
#[derive(Debug)]
pub struct CommittedResearchMarketObservationLease {
    observation: CommittedResearchMarketObservation,
    _retained_budget: OwnedSemaphorePermit,
}

impl CommittedResearchMarketObservationLease {
    pub const fn observation(&self) -> &CommittedResearchMarketObservation {
        &self.observation
    }

    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self._retained_budget.num_permits()
    }

    #[must_use]
    pub fn into_observation(self) -> CommittedResearchMarketObservation {
        self.observation
    }
}

/// Sole read-only consumer for one route's committed research observations.
#[derive(Debug)]
pub struct CommittedResearchMarketObservationReceiver {
    receiver: mpsc::Receiver<CommittedResearchMarketObservationLease>,
}

impl CommittedResearchMarketObservationReceiver {
    pub async fn recv(&mut self) -> Option<CommittedResearchMarketObservationLease> {
        self.receiver.recv().await
    }

    pub fn try_recv(
        &mut self,
    ) -> Result<CommittedResearchMarketObservationLease, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommittedResearchMarketExportDisposition {
    Full,
    Closed,
}
