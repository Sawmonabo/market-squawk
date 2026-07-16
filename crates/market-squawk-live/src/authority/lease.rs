//! One-way revocation and checked state-revision allocations.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use thiserror::Error;

#[derive(Debug)]
struct OneWayLeaseState {
    allocation_id: u64,
    active: AtomicBool,
}

/// Sole owner of degradation authority for one allocation.
#[derive(Debug)]
pub(crate) struct OneWayLeaseOwner<D> {
    state: Arc<OneWayLeaseState>,
    dimension: PhantomData<fn() -> D>,
}

impl<D> OneWayLeaseOwner<D> {
    pub(crate) fn new(allocation_id: u64) -> Self {
        Self {
            state: Arc::new(OneWayLeaseState {
                allocation_id,
                active: AtomicBool::new(true),
            }),
            dimension: PhantomData,
        }
    }

    pub(crate) fn lease(&self) -> OneWayLease<D> {
        OneWayLease {
            state: Arc::clone(&self.state),
            dimension: PhantomData,
        }
    }

    pub(crate) fn invalidate(&mut self) {
        self.state.active.store(false, Ordering::Release);
    }
}

impl<D> Drop for OneWayLeaseOwner<D> {
    fn drop(&mut self) {
        self.invalidate();
    }
}

/// O(1)-clone validation-only view of a one-way allocation.
#[derive(Debug)]
pub(crate) struct OneWayLease<D> {
    state: Arc<OneWayLeaseState>,
    dimension: PhantomData<fn() -> D>,
}

impl<D> Clone for OneWayLease<D> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            dimension: PhantomData,
        }
    }
}

impl<D> OneWayLease<D> {
    pub(crate) fn validate(&self) -> Result<(), LeaseError> {
        if self.state.active.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(LeaseError::Revoked)
        }
    }

    pub(crate) fn shares_allocation_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
            && self.state.allocation_id == other.state.allocation_id
    }

    /// Publishes one-way degradation. Validation-only holders can only reduce authority.
    pub(crate) fn invalidate(&self) {
        self.state.active.store(false, Ordering::Release);
    }
}

#[derive(Debug)]
pub(crate) enum GenerationExecution {}
#[derive(Debug)]
pub(crate) enum ShardLiveness {}
#[derive(Debug)]
pub(crate) enum RuntimeIncarnation {}
#[derive(Debug)]
pub(crate) enum TradingStatusAuthority {}
#[derive(Debug)]
pub(crate) enum GenerationRegistryLifecycle {}

pub(crate) type GenerationLease = OneWayLease<GenerationExecution>;
pub(crate) type GenerationLeaseOwner = OneWayLeaseOwner<GenerationExecution>;
pub(crate) type ShardLease = OneWayLease<ShardLiveness>;
pub(crate) type ShardLeaseOwner = OneWayLeaseOwner<ShardLiveness>;
pub(crate) type RuntimeLease = OneWayLease<RuntimeIncarnation>;
pub(crate) type RuntimeLeaseOwner = OneWayLeaseOwner<RuntimeIncarnation>;
pub(crate) type StatusLease = OneWayLease<TradingStatusAuthority>;
pub(crate) type StatusLeaseOwner = OneWayLeaseOwner<TradingStatusAuthority>;
pub(crate) type RegistryLifecycleLease = OneWayLease<GenerationRegistryLifecycle>;
pub(crate) type RegistryLifecycleOwner = OneWayLeaseOwner<GenerationRegistryLifecycle>;

#[derive(Debug)]
struct StateRevisionState {
    active: AtomicBool,
    revision: AtomicU64,
}

/// Sole mutator of one instrument-state revision allocation.
#[derive(Debug)]
pub(crate) struct StateRevisionOwner<D> {
    state: Arc<StateRevisionState>,
    dimension: PhantomData<fn() -> D>,
}

impl<D> StateRevisionOwner<D> {
    pub(crate) fn new() -> Self {
        Self::from_revision(0)
    }

    fn from_revision(revision: u64) -> Self {
        Self {
            state: Arc::new(StateRevisionState {
                active: AtomicBool::new(true),
                revision: AtomicU64::new(revision),
            }),
            dimension: PhantomData,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(revision: u64) -> Self {
        Self::from_revision(revision)
    }

    /// Returns the last diagnostic revision, including after fail-closed invalidation.
    pub(crate) fn diagnostic_revision(&self) -> u64 {
        self.state.revision.load(Ordering::Acquire)
    }

    pub(crate) fn advance(&mut self) -> Result<u64, LeaseError> {
        if !self.state.active.load(Ordering::Acquire) {
            return Err(LeaseError::Revoked);
        }
        let current = self.state.revision.load(Ordering::Acquire);
        let Some(next) = current.checked_add(1) else {
            self.state.active.store(false, Ordering::Release);
            return Err(LeaseError::RevisionExhausted);
        };
        self.state.revision.store(next, Ordering::Release);
        Ok(next)
    }

    pub(crate) fn lease(&self) -> StateRevisionLease<D> {
        StateRevisionLease {
            state: Arc::clone(&self.state),
            dimension: PhantomData,
        }
    }

    pub(crate) fn invalidate(&mut self) {
        self.state.active.store(false, Ordering::Release);
    }
}

/// Validation-only view of an exact committed state revision.
#[derive(Debug)]
pub(crate) struct StateRevisionLease<D> {
    state: Arc<StateRevisionState>,
    dimension: PhantomData<fn() -> D>,
}

impl<D> Clone for StateRevisionLease<D> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            dimension: PhantomData,
        }
    }
}

impl<D> StateRevisionLease<D> {
    pub(crate) fn validate(&self, expected: u64) -> Result<(), LeaseError> {
        let revision = self.state.revision.load(Ordering::Acquire);
        let active = self.state.active.load(Ordering::Acquire);
        if !active {
            Err(LeaseError::Revoked)
        } else if revision != expected {
            Err(LeaseError::StaleRevision {
                expected,
                current: revision,
            })
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
pub(crate) enum StreamStateRevision {}
#[derive(Debug)]
pub(crate) enum StatusStateRevision {}

pub(crate) type StreamRevisionOwner = StateRevisionOwner<StreamStateRevision>;
pub(crate) type StreamRevisionLease = StateRevisionLease<StreamStateRevision>;
pub(crate) type StatusRevisionOwner = StateRevisionOwner<StatusStateRevision>;
pub(crate) type StatusRevisionLease = StateRevisionLease<StatusStateRevision>;

/// One-way lease validation failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum LeaseError {
    #[error("authority allocation is revoked")]
    Revoked,
    #[error("state revision is stale: expected {expected}, current {current}")]
    StaleRevision { expected: u64, current: u64 },
    #[error("state revision counter exhausted")]
    RevisionExhausted,
}
