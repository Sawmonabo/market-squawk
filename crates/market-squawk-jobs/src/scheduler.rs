use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex, MutexGuard},
};

use market_squawk_domain::SourceIdentifier;
use thiserror::Error;
use tokio::sync::Notify;

use crate::{JobActivitySnapshot, JobGeneration, JobId};

/// Global and per-kind queue/running ceilings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulerLimits {
    maximum_queued: usize,
    maximum_running: usize,
    maximum_queued_per_kind: usize,
    maximum_running_per_kind: usize,
}

impl SchedulerLimits {
    /// Creates positive ceilings whose per-kind values do not exceed the global values.
    pub fn try_new(
        maximum_queued: usize,
        maximum_running: usize,
        maximum_queued_per_kind: usize,
        maximum_running_per_kind: usize,
    ) -> Result<Self, SchedulerError> {
        if maximum_queued == 0
            || maximum_running == 0
            || maximum_queued_per_kind == 0
            || maximum_running_per_kind == 0
            || maximum_queued_per_kind > maximum_queued
            || maximum_running_per_kind > maximum_running
        {
            return Err(SchedulerError::InvalidLimits);
        }
        Ok(Self {
            maximum_queued,
            maximum_running,
            maximum_queued_per_kind,
            maximum_running_per_kind,
        })
    }
}

/// Exact scheduled execution generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledJob {
    id: JobId,
    generation: JobGeneration,
    kind: SourceIdentifier,
}

impl ScheduledJob {
    /// Creates a path-free queue item.
    #[must_use]
    pub const fn new(id: JobId, generation: JobGeneration, kind: SourceIdentifier) -> Self {
        Self {
            id,
            generation,
            kind,
        }
    }

    /// Stable job identity.
    #[must_use]
    pub const fn id(&self) -> JobId {
        self.id
    }

    /// Exact execution generation.
    #[must_use]
    pub const fn generation(&self) -> JobGeneration {
        self.generation
    }

    /// Registered runner kind.
    #[must_use]
    pub const fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }
}

/// Scheduler admission failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SchedulerError {
    /// Configured queue or execution ceilings are invalid.
    #[error("scheduler limits are invalid")]
    InvalidLimits,
    /// Global or per-kind queue capacity is exhausted.
    #[error("scheduler queue is full")]
    QueueFull,
    /// Shutdown raced a durable mutation that still owns reserved queue capacity.
    #[error("scheduler has active durable-mutation reservations")]
    ReservationsActive,
}

/// Fair bounded scheduler with round-robin kind admission.
#[derive(Clone, Debug)]
pub struct FairJobScheduler {
    inner: Arc<SchedulerInner>,
}

#[derive(Debug)]
struct SchedulerInner {
    limits: SchedulerLimits,
    state: Mutex<SchedulerState>,
    notify: Notify,
}

#[derive(Debug, Default)]
struct SchedulerState {
    queues: BTreeMap<SourceIdentifier, VecDeque<ScheduledJob>>,
    kinds: VecDeque<SourceIdentifier>,
    queued: usize,
    running: usize,
    running_by_kind: BTreeMap<SourceIdentifier, usize>,
    running_jobs: BTreeMap<(JobId, JobGeneration), SourceIdentifier>,
    reserved: usize,
    reserved_by_kind: BTreeMap<SourceIdentifier, usize>,
    closed: bool,
    snapshot_fenced: bool,
}

impl FairJobScheduler {
    /// Creates an empty scheduler under fixed ceilings.
    #[must_use]
    pub fn new(limits: SchedulerLimits) -> Self {
        Self {
            inner: Arc::new(SchedulerInner {
                limits,
                state: Mutex::new(SchedulerState::default()),
                notify: Notify::new(),
            }),
        }
    }

    /// Enqueues without waiting when both global and kind capacity remain.
    pub async fn enqueue(&self, job: ScheduledJob) -> Result<(), SchedulerError> {
        self.reserve(job.kind().clone())?.commit(job)
    }

    /// Reserves exact global and per-kind capacity before a durable job mutation.
    pub fn reserve(&self, kind: SourceIdentifier) -> Result<JobQueueReservation, SchedulerError> {
        let mut state = self.inner.lock_state();
        let kind_count = state.queues.get(&kind).map_or(0, VecDeque::len)
            + state.reserved_by_kind.get(&kind).copied().unwrap_or(0);
        if state.closed
            || state.snapshot_fenced
            || state.queued + state.reserved >= self.inner.limits.maximum_queued
            || kind_count >= self.inner.limits.maximum_queued_per_kind
        {
            return Err(SchedulerError::QueueFull);
        }
        state.reserved += 1;
        *state.reserved_by_kind.entry(kind.clone()).or_default() += 1;
        Ok(JobQueueReservation {
            kind,
            scheduler: self.inner.clone(),
            committed: false,
        })
    }

    fn commit_reserved(&self, kind: &SourceIdentifier, job: ScheduledJob) {
        let mut state = self.inner.lock_state();
        release_reservation(&mut state, kind);
        let was_empty = state.queues.get(kind).is_none_or(VecDeque::is_empty);
        if was_empty {
            state.kinds.push_back(kind.clone());
        }
        state.queues.entry(kind.clone()).or_default().push_back(job);
        state.queued += 1;
        drop(state);
        self.inner.notify.notify_one();
    }

    /// Waits for the next fairly admitted job or returns `None` after scheduler close.
    pub async fn next(&self) -> Option<JobLease> {
        loop {
            let notified = self.inner.notify.notified();
            {
                let mut state = self.inner.lock_state();
                if state.closed && state.queued == 0 {
                    return None;
                }
                if !state.snapshot_fenced && state.running < self.inner.limits.maximum_running {
                    let visits = state.kinds.len();
                    for _ in 0..visits {
                        let Some(kind) = state.kinds.pop_front() else {
                            break;
                        };
                        let running = state.running_by_kind.get(&kind).copied().unwrap_or(0);
                        if running >= self.inner.limits.maximum_running_per_kind {
                            state.kinds.push_back(kind);
                            continue;
                        }
                        let job = state.queues.get_mut(&kind).and_then(VecDeque::pop_front);
                        let Some(job) = job else {
                            state.queues.remove(&kind);
                            continue;
                        };
                        let queue_empty = state.queues.get(&kind).is_none_or(VecDeque::is_empty);
                        if queue_empty {
                            state.queues.remove(&kind);
                        } else {
                            state.kinds.push_back(kind.clone());
                        }
                        state.queued -= 1;
                        state.running += 1;
                        *state.running_by_kind.entry(kind.clone()).or_default() += 1;
                        state
                            .running_jobs
                            .insert((job.id(), job.generation()), kind.clone());
                        return Some(JobLease {
                            job,
                            kind,
                            scheduler: self.inner.clone(),
                            released: false,
                        });
                    }
                }
            }
            notified.await;
        }
    }

    pub(crate) fn activity(
        &self,
        mutation_kinds: &BTreeSet<SourceIdentifier>,
    ) -> JobActivitySnapshot {
        let state = self.inner.lock_state();
        let running_mutations = mutation_kinds
            .iter()
            .filter_map(|kind| state.running_by_kind.get(kind))
            .copied()
            .sum();
        JobActivitySnapshot::new(state.running, running_mutations)
    }

    pub(crate) fn retain_exclusive(
        &self,
        active_kind: &SourceIdentifier,
    ) -> Option<(ScheduledJob, SchedulerSnapshotFence)> {
        let mut state = self.inner.lock_state();
        if state.closed
            || state.snapshot_fenced
            || state.reserved != 0
            || state.queued != 0
            || state.running_jobs.len() != 1
        {
            return None;
        }
        let (&(id, generation), kind) = state.running_jobs.first_key_value()?;
        if kind != active_kind {
            return None;
        }
        let kind = kind.clone();
        state.snapshot_fenced = true;
        Some((
            ScheduledJob::new(id, generation, kind),
            SchedulerSnapshotFence {
                scheduler: self.inner.clone(),
                released: false,
            },
        ))
    }

    /// Prevents new work, removes queued generations, and wakes blocked claimers.
    pub fn close(&self) -> Result<Vec<ScheduledJob>, SchedulerError> {
        let mut state = self.inner.lock_state();
        if state.reserved != 0 || state.snapshot_fenced {
            return Err(SchedulerError::ReservationsActive);
        }
        state.closed = true;
        let mut queued = Vec::with_capacity(state.queued);
        for queue in state.queues.values_mut() {
            queued.extend(queue.drain(..));
        }
        state.queues.clear();
        state.kinds.clear();
        state.queued = 0;
        drop(state);
        self.inner.notify.notify_waiters();
        Ok(queued)
    }
}

impl SchedulerInner {
    fn lock_state(&self) -> MutexGuard<'_, SchedulerState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

fn release_reservation(state: &mut SchedulerState, kind: &SourceIdentifier) {
    state.reserved = state.reserved.saturating_sub(1);
    if let Some(reserved) = state.reserved_by_kind.get_mut(kind) {
        *reserved = reserved.saturating_sub(1);
        if *reserved == 0 {
            state.reserved_by_kind.remove(kind);
        }
    }
}

/// Exact scheduler capacity held across one durable authority mutation.
#[derive(Debug)]
pub struct JobQueueReservation {
    kind: SourceIdentifier,
    scheduler: Arc<SchedulerInner>,
    committed: bool,
}

impl JobQueueReservation {
    /// Publishes the already-durable generation into its reserved fair queue slot.
    pub fn commit(mut self, job: ScheduledJob) -> Result<(), SchedulerError> {
        if job.kind() != &self.kind {
            return Err(SchedulerError::InvalidLimits);
        }
        FairJobScheduler {
            inner: self.scheduler.clone(),
        }
        .commit_reserved(&self.kind, job);
        self.committed = true;
        Ok(())
    }
}

impl Drop for JobQueueReservation {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let mut state = self.scheduler.lock_state();
        release_reservation(&mut state, &self.kind);
        drop(state);
        self.scheduler.notify.notify_waiters();
    }
}

/// Running-capacity lease released on completion or cancellation.
#[derive(Debug)]
pub struct JobLease {
    job: ScheduledJob,
    kind: SourceIdentifier,
    scheduler: Arc<SchedulerInner>,
    released: bool,
}

impl JobLease {
    /// Exact admitted job generation.
    #[must_use]
    pub const fn job(&self) -> &ScheduledJob {
        &self.job
    }

    /// Releases capacity and wakes the next fair claimant.
    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        let mut state = self.scheduler.lock_state();
        state.running = state.running.saturating_sub(1);
        if let Some(running) = state.running_by_kind.get_mut(&self.kind) {
            *running = running.saturating_sub(1);
            if *running == 0 {
                state.running_by_kind.remove(&self.kind);
            }
        }
        state
            .running_jobs
            .remove(&(self.job.id(), self.job.generation()));
        self.released = true;
        drop(state);
        self.scheduler.notify.notify_one();
    }
}

impl Drop for JobLease {
    fn drop(&mut self) {
        self.release_inner();
    }
}

/// Scheduler dispatch fence retained across one coherent logical snapshot.
#[derive(Debug)]
pub(crate) struct SchedulerSnapshotFence {
    scheduler: Arc<SchedulerInner>,
    released: bool,
}

impl SchedulerSnapshotFence {
    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        let mut state = self.scheduler.lock_state();
        state.snapshot_fenced = false;
        self.released = true;
        drop(state);
        self.scheduler.notify.notify_waiters();
    }
}

impl Drop for SchedulerSnapshotFence {
    fn drop(&mut self) {
        self.release_inner();
    }
}
