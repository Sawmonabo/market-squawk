//! Bounded export of non-executable committed market observations for durable research.

use std::mem::size_of;
use std::num::NonZeroUsize;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use market_squawk_domain::SourceIdentifier;
use market_squawk_sources::{
    CurrentDecodedProviderBatch, CurrentObservationEvidence, CurrentProviderObservation,
};

use crate::{CommittedResearchMarketObservation, ShardKey};

const MAX_EXPORT_BATCHES: usize = 65_536;
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

/// Route-owned sender bounded by terminal batch count and retained bytes.
#[derive(Debug)]
pub struct RouteCommittedResearchMarketExport {
    route: ShardKey,
    sender: mpsc::Sender<CommittedResearchMarketBatchLease>,
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
            || capacity > MAX_EXPORT_BATCHES
            || maximum_retained_bytes == 0
            || maximum_retained_bytes > MAX_EXPORT_RETAINED_BYTES
        {
            return Err(CommittedResearchMarketExportError::InvalidCapacity);
        }
        let slot_bytes = capacity
            .checked_mul(size_of::<CommittedResearchMarketBatchLease>())
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

    /// Reserves a complete route-batch outcome before any observation mutates live state.
    pub(crate) fn prepare(
        &self,
        batch: &CurrentDecodedProviderBatch,
        conservative_batch_bytes: u32,
    ) -> Result<PreparedCommittedResearchMarketBatch, CommittedResearchMarketExportDisposition>
    {
        use CommittedResearchMarketExportDisposition as Error;
        let observations = batch.observations();
        let first = observations.first().ok_or(Error::Invalid)?;
        let row_count = first.row_count();
        if row_count == 0 || row_count > market_squawk_sources::MAX_DECODED_EVENTS {
            return Err(Error::Invalid);
        }
        // Admission already charges every input payload/identity and the shared source evidence
        // and authority once. Canonical qualification adds owned binding strings per row, while
        // terminal coordinates and leases add inline/vector storage. Never multiply the complete
        // admitted batch by its row count.
        let overhead = size_of::<CommittedResearchMarketBatchLease>()
            .checked_add(size_of::<OwnedSemaphorePermit>())
            .and_then(|bytes| bytes.checked_add(2 * size_of::<usize>()))
            .and_then(|bytes| {
                bytes.checked_add(observations.len().checked_mul(
                    size_of::<usize>() + size_of::<CommittedResearchMarketObservationLease>(),
                )?)
            })
            .ok_or(Error::Invalid)?;
        let total = observations.iter().try_fold(
            (conservative_batch_bytes as usize)
                .checked_add(overhead)
                .ok_or(Error::Invalid)?,
            |bytes, observation| {
                bytes
                    .checked_add(
                        qualification_allocation_charge(observation).ok_or(Error::Invalid)?,
                    )
                    .ok_or(Error::Invalid)
            },
        )?;
        // Keep each row's share conservative when a consumer retains only part of the batch.
        // All shares refer to one permit; rounding adds fewer than one byte per row.
        let row_charge = total.div_ceil(observations.len());
        let total = row_charge
            .checked_mul(observations.len())
            .and_then(|bytes| u32::try_from(bytes).ok())
            .ok_or(Error::Invalid)?;
        let retained_budget = Arc::new(
            Arc::clone(&self.retained_budget)
                .try_acquire_many_owned(total)
                .map_err(|error| match error {
                    tokio::sync::TryAcquireError::NoPermits => Error::Full,
                    tokio::sync::TryAcquireError::Closed => Error::Closed,
                })?,
        );
        let slot = self
            .sender
            .clone()
            .try_reserve_owned()
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::Full,
                mpsc::error::TrySendError::Closed(_) => Error::Closed,
            })?;
        let mut ordinals = Vec::new();
        ordinals
            .try_reserve_exact(observations.len())
            .map_err(|_| Error::Invalid)?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(observations.len())
            .map_err(|_| Error::Invalid)?;
        for observation in observations {
            let ordinal = observation.row_ordinal();
            if observation.evidence() != first.evidence()
                || observation.row_count() != row_count
                || ordinal >= row_count
                || ordinals.last().is_some_and(|previous| *previous >= ordinal)
            {
                return Err(Error::Invalid);
            }
            ordinals.push(ordinal);
        }
        Ok(PreparedCommittedResearchMarketBatch {
            slot,
            coordinates: CommittedResearchMarketBatchCoordinates {
                evidence: first.evidence().clone(),
                row_count,
                wire_ordinals: ordinals,
            },
            rows,
            row_charge,
            retained_budget,
        })
    }
}

// Mirrors the owned allocations produced by qualification::build_qualified_event. Payload
// vectors, provider identity and shared source custody are covered by the admitted batch charge.
fn qualification_allocation_charge(current: &CurrentProviderObservation) -> Option<usize> {
    let observation = current.observation();
    let policy = current.policy();
    let binding = current.evidence().binding();
    let binding_strings = [
        binding.source_id().retained_bytes(),
        binding.session_id().as_source_identifier().retained_bytes(),
        binding
            .metadata_revision()
            .as_source_identifier()
            .retained_bytes(),
        policy
            .static_authorization()
            .basis()
            .as_source_identifier()
            .retained_bytes(),
        observation.venue().retained_bytes(),
        policy
            .provider_product()
            .as_source_identifier()
            .retained_bytes(),
        policy
            .provider_channel()
            .as_source_identifier()
            .retained_bytes(),
        observation.source_identifier().retained_bytes(),
    ]
    .into_iter()
    .try_fold(0usize, usize::checked_add)?;
    // State is not committed yet: bound its generated canonical rule, and for book events the
    // current/snapshot state identifiers and their rules, by the validated identifier limit.
    let state_identifiers = if observation.event_class().requires_book_state() {
        5usize
    } else {
        1
    };
    let binding_bytes = binding_strings
        .checked_add(state_identifiers.checked_mul(SourceIdentifier::MAX_LENGTH)?)?;
    // One root + one policy + four integrity + six market + one nested coverage + provenance.
    let bindings = binding_bytes.checked_mul(1 + 1 + 4 + 6 + 1 + 1)?;
    let coverage = policy.coverage();
    let coverage_strings = [
        coverage.source_id().retained_bytes(),
        coverage.venue().retained_bytes(),
        coverage
            .provider_product()
            .as_source_identifier()
            .retained_bytes(),
        coverage
            .provider_channel()
            .as_source_identifier()
            .retained_bytes(),
        coverage
            .metadata_revision()
            .as_source_identifier()
            .retained_bytes(),
    ]
    .into_iter()
    .try_fold(0usize, usize::checked_add)?;
    // Assessment identity is owned by both assessment and provenance. Snapshot evidence can
    // additionally own an initialized-snapshot identity and canonical rule. Cloned identifiers
    // retain their validated bytes, independently of spare capacity in the originating value.
    bindings
        .checked_add(coverage_strings)?
        .checked_add(
            policy
                .rule()
                .snapshot_applicability()
                .dynamic_retained_bytes()?,
        )?
        .checked_add(4usize.checked_mul(SourceIdentifier::MAX_LENGTH)?)
}

/// Exact captured source-object coordinates covered by one terminal route batch.
/// No constructor or deserializer is exposed; these coordinates cannot mint canonical rows.
#[derive(Debug)]
pub struct CommittedResearchMarketBatchCoordinates {
    evidence: CurrentObservationEvidence,
    row_count: usize,
    wire_ordinals: Vec<usize>,
}

impl CommittedResearchMarketBatchCoordinates {
    pub const fn evidence(&self) -> &CurrentObservationEvidence {
        &self.evidence
    }
    pub const fn row_count(&self) -> usize {
        self.row_count
    }
    pub fn wire_ordinals(&self) -> &[usize] {
        &self.wire_ordinals
    }
}

/// Whole-route terminal disposition. A rejected batch exposes no partial canonical prefix.
#[derive(Debug)]
pub enum CommittedResearchMarketBatchOutcome {
    Committed(Vec<CommittedResearchMarketObservationLease>),
    Rejected,
}

/// One atomically exported route batch, sharing a byte lease across every committed row.
#[derive(Debug)]
pub struct CommittedResearchMarketBatchLease {
    coordinates: CommittedResearchMarketBatchCoordinates,
    outcome: CommittedResearchMarketBatchOutcome,
    retained_budget: Arc<OwnedSemaphorePermit>,
}

impl CommittedResearchMarketBatchLease {
    pub const fn coordinates(&self) -> &CommittedResearchMarketBatchCoordinates {
        &self.coordinates
    }
    pub fn retained_bytes(&self) -> usize {
        self.retained_budget.num_permits()
    }
    pub fn into_parts(
        self,
    ) -> (
        CommittedResearchMarketBatchCoordinates,
        CommittedResearchMarketBatchOutcome,
    ) {
        (self.coordinates, self.outcome)
    }
}

pub(crate) struct PreparedCommittedResearchMarketBatch {
    slot: mpsc::OwnedPermit<CommittedResearchMarketBatchLease>,
    coordinates: CommittedResearchMarketBatchCoordinates,
    rows: Vec<CommittedResearchMarketObservationLease>,
    row_charge: usize,
    retained_budget: Arc<OwnedSemaphorePermit>,
}

impl PreparedCommittedResearchMarketBatch {
    pub(crate) fn push(
        &mut self,
        observation: CommittedResearchMarketObservation,
    ) -> Result<(), CommittedResearchMarketExportDisposition> {
        if self.coordinates.wire_ordinals.get(self.rows.len()).copied()
            != Some(observation.wire_ordinal())
            || self.coordinates.row_count != observation.row_count()
        {
            return Err(CommittedResearchMarketExportDisposition::Invalid);
        }
        self.rows.push(CommittedResearchMarketObservationLease {
            observation,
            retained_bytes: self.row_charge,
            _retained_budget: Arc::clone(&self.retained_budget),
        });
        Ok(())
    }

    pub(crate) fn finish(self, succeeded: bool) {
        let outcome = if succeeded && self.rows.len() == self.coordinates.wire_ordinals.len() {
            CommittedResearchMarketBatchOutcome::Committed(self.rows)
        } else {
            CommittedResearchMarketBatchOutcome::Rejected
        };
        drop(self.slot.send(CommittedResearchMarketBatchLease {
            coordinates: self.coordinates,
            outcome,
            retained_budget: self.retained_budget,
        }));
    }
}

/// One consumer-owned committed observation and its retained-byte reservation.
#[derive(Debug)]
pub struct CommittedResearchMarketObservationLease {
    observation: CommittedResearchMarketObservation,
    retained_bytes: usize,
    _retained_budget: Arc<OwnedSemaphorePermit>,
}

impl CommittedResearchMarketObservationLease {
    pub const fn observation(&self) -> &CommittedResearchMarketObservation {
        &self.observation
    }

    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    #[must_use]
    pub fn into_observation(self) -> CommittedResearchMarketObservation {
        self.observation
    }
}

/// Sole consumer of complete or rejected research batches for one route.
#[derive(Debug)]
pub struct CommittedResearchMarketObservationReceiver {
    receiver: mpsc::Receiver<CommittedResearchMarketBatchLease>,
}

impl CommittedResearchMarketObservationReceiver {
    pub async fn recv(&mut self) -> Option<CommittedResearchMarketBatchLease> {
        self.receiver.recv().await
    }

    pub fn try_recv(
        &mut self,
    ) -> Result<CommittedResearchMarketBatchLease, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommittedResearchMarketExportDisposition {
    Full,
    Closed,
    Invalid,
}
