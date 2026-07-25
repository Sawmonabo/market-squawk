use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use market_squawk_domain::MarketEvent;
use market_squawk_execution::{
    BoundedOrderIntents, ExecutionLiveActionHook, Strategy, StrategyContext, StrategyError,
};
use market_squawk_live::{
    ActionAuthorityIssueLimit, ActionHookDisposition, CommittedActionContext, CurrentAuthorityGate,
    LiveActionHook, LiveActionHookError,
};
use tokio::sync::Notify;

use super::ReleaseLatencyDistribution;

const HISTOGRAM_BUCKET_NANOS: u64 = 1_000;
const HISTOGRAM_BUCKETS: usize = 2_002;
pub(super) const PHASE_IDLE: u8 = 0;
pub(super) const PHASE_MEASURE: u8 = 1;
pub(super) const PHASE_DISPATCH: u8 = 2;

#[derive(Debug)]
pub(crate) struct ObservedExecutionHook {
    inner: ExecutionLiveActionHook,
    observer: Arc<ReleaseBenchmarkObserver>,
}

impl ObservedExecutionHook {
    pub(crate) fn new(
        inner: ExecutionLiveActionHook,
        observer: Arc<ReleaseBenchmarkObserver>,
    ) -> Self {
        Self { inner, observer }
    }

    pub(super) fn retained_overhead_bytes() -> Result<usize> {
        size_of::<Self>()
            .checked_sub(size_of::<ExecutionLiveActionHook>())
            .context("observed execution-hook retained accounting underflowed")
    }
}

impl LiveActionHook for ObservedExecutionHook {
    fn on_committed(
        &mut self,
        context: CommittedActionContext<'_>,
        authority: &mut CurrentAuthorityGate<'_>,
    ) -> ActionHookDisposition {
        let disposition = self.inner.on_committed(context, authority);
        self.observer.record_action(disposition);
        disposition
    }

    fn retained_bytes(&self) -> Result<usize, LiveActionHookError> {
        self.inner
            .retained_bytes()?
            .checked_add(
                Self::retained_overhead_bytes()
                    .map_err(|_| LiveActionHookError::RetainedSizeOverflow)?,
            )
            .ok_or(LiveActionHookError::RetainedSizeOverflow)
    }

    fn maximum_authority_issues(&self) -> ActionAuthorityIssueLimit {
        self.inner.maximum_authority_issues()
    }
}

#[derive(Debug)]
pub(super) struct ObservedStrategy {
    inner: Box<dyn Strategy>,
    observer: Arc<ReleaseBenchmarkObserver>,
}

impl ObservedStrategy {
    pub(super) fn new(inner: Box<dyn Strategy>, observer: Arc<ReleaseBenchmarkObserver>) -> Self {
        Self { inner, observer }
    }
}

impl Strategy for ObservedStrategy {
    fn on_market_event(
        &mut self,
        context: &StrategyContext<'_>,
        event: &MarketEvent,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        let result = self.inner.on_market_event(context, event);
        self.observer.record_strategy();
        result
    }

    fn retained_bytes(&self) -> Result<usize, StrategyError> {
        self.inner
            .retained_bytes()?
            .checked_add(
                size_of::<Self>()
                    .checked_sub(size_of::<Box<dyn Strategy>>())
                    .and_then(|value| value.checked_add(self.observer.retained_bytes().ok()?))
                    .ok_or(StrategyError::RetainedSize)?,
            )
            .ok_or(StrategyError::RetainedSize)
    }
}

#[derive(Debug)]
pub(crate) struct ReleaseBenchmarkObserver {
    epoch: Instant,
    batch_started_nanos: AtomicU64,
    batch_target: AtomicU64,
    completed: AtomicU64,
    phase: AtomicU8,
    strategy: LatencyHistogram,
    action: LatencyHistogram,
    dispatch_strategy_nanos: AtomicU64,
    dispatch_action_nanos: AtomicU64,
    dispatch_disposition: AtomicU8,
    notify: Notify,
}

impl ReleaseBenchmarkObserver {
    pub(super) fn try_new() -> Result<Self> {
        Ok(Self {
            epoch: Instant::now(),
            batch_started_nanos: AtomicU64::new(0),
            batch_target: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            phase: AtomicU8::new(PHASE_IDLE),
            strategy: LatencyHistogram::try_new()?,
            action: LatencyHistogram::try_new()?,
            dispatch_strategy_nanos: AtomicU64::new(0),
            dispatch_action_nanos: AtomicU64::new(0),
            dispatch_disposition: AtomicU8::new(0),
            notify: Notify::new(),
        })
    }

    pub(super) fn retained_bytes(&self) -> Result<usize> {
        size_of::<Self>()
            .checked_add(self.strategy.dynamic_retained_bytes()?)
            .and_then(|value| value.checked_add(self.action.dynamic_retained_bytes().ok()?))
            .context("release benchmark observer retained bytes overflowed")
    }

    pub(super) fn set_phase(&self, phase: u8) {
        self.phase.store(phase, Ordering::Release);
    }

    pub(super) fn reset_measurement(&self) {
        self.strategy.reset();
        self.action.reset();
    }

    pub(super) fn begin_batch(&self, events: u64) -> Result<u64> {
        let target = self
            .completed
            .load(Ordering::Acquire)
            .checked_add(events)
            .context("release benchmark observer count overflowed")?;
        self.batch_started_nanos
            .store(self.now_nanos(), Ordering::Release);
        self.batch_target.store(target, Ordering::Release);
        Ok(target)
    }

    pub(super) async fn wait_for(&self, target: u64, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .context("release benchmark observer deadline overflowed")?;
        loop {
            let notified = self.notify.notified();
            if self.completed.load(Ordering::Acquire) >= target {
                return Ok(());
            }
            tokio::time::timeout_at(deadline, notified)
                .await
                .context("release benchmark live batch exceeded its processing deadline")?;
        }
    }

    pub(super) fn strategy_distribution(
        &self,
        elapsed_nanos: u64,
    ) -> Result<ReleaseLatencyDistribution> {
        self.strategy.distribution(elapsed_nanos)
    }

    pub(super) fn action_distribution(
        &self,
        elapsed_nanos: u64,
    ) -> Result<ReleaseLatencyDistribution> {
        self.action.distribution(elapsed_nanos)
    }

    pub(super) fn dispatch_strategy_nanos(&self) -> u64 {
        self.dispatch_strategy_nanos.load(Ordering::Acquire)
    }

    pub(super) fn dispatch_action_nanos(&self) -> u64 {
        self.dispatch_action_nanos.load(Ordering::Acquire)
    }

    pub(super) fn dispatch_disposition(&self) -> &'static str {
        match self.dispatch_disposition.load(Ordering::Acquire) {
            1 => "no_action",
            2 => "suppressed",
            3 => "dispatched",
            4 => "failed",
            _ => "not_observed",
        }
    }

    fn record_strategy(&self) {
        let elapsed = self.batch_elapsed_nanos();
        match self.phase.load(Ordering::Acquire) {
            PHASE_MEASURE => self.strategy.record(elapsed),
            PHASE_DISPATCH => self
                .dispatch_strategy_nanos
                .store(elapsed, Ordering::Release),
            _ => {}
        }
    }

    fn record_action(&self, disposition: ActionHookDisposition) {
        let elapsed = self.batch_elapsed_nanos();
        match self.phase.load(Ordering::Acquire) {
            PHASE_MEASURE => self.action.record(elapsed),
            PHASE_DISPATCH => {
                self.dispatch_action_nanos.store(elapsed, Ordering::Release);
                self.dispatch_disposition
                    .store(disposition_code(disposition), Ordering::Release);
            }
            _ => {}
        }
        let completed = self.completed.fetch_add(1, Ordering::AcqRel) + 1;
        if completed >= self.batch_target.load(Ordering::Acquire) {
            self.notify.notify_one();
        }
    }

    fn batch_elapsed_nanos(&self) -> u64 {
        self.now_nanos()
            .saturating_sub(self.batch_started_nanos.load(Ordering::Acquire))
    }

    fn now_nanos(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

#[derive(Debug)]
struct LatencyHistogram {
    buckets: Box<[AtomicU64]>,
    count: AtomicU64,
    maximum: AtomicU64,
}

impl LatencyHistogram {
    fn try_new() -> Result<Self> {
        let mut buckets = Vec::new();
        buckets.try_reserve_exact(HISTOGRAM_BUCKETS)?;
        buckets.extend((0..HISTOGRAM_BUCKETS).map(|_| AtomicU64::new(0)));
        Ok(Self {
            buckets: buckets.into_boxed_slice(),
            count: AtomicU64::new(0),
            maximum: AtomicU64::new(0),
        })
    }

    fn record(&self, nanos: u64) {
        let raw = nanos / HISTOGRAM_BUCKET_NANOS;
        let index = usize::try_from(raw)
            .unwrap_or(usize::MAX)
            .min(self.buckets.len().saturating_sub(1));
        if let Some(bucket) = self.buckets.get(index) {
            bucket.fetch_add(1, Ordering::Relaxed);
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.maximum.fetch_max(nanos, Ordering::Relaxed);
    }

    fn reset(&self) {
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
        self.count.store(0, Ordering::Relaxed);
        self.maximum.store(0, Ordering::Relaxed);
    }

    fn dynamic_retained_bytes(&self) -> Result<usize> {
        self.buckets
            .len()
            .checked_mul(size_of::<AtomicU64>())
            .context("release benchmark histogram retained bytes overflowed")
    }

    fn distribution(&self, elapsed_nanos: u64) -> Result<ReleaseLatencyDistribution> {
        let operations = self.count.load(Ordering::Acquire);
        if operations == 0 || elapsed_nanos == 0 {
            bail!("release benchmark latency distribution is empty");
        }
        Ok(ReleaseLatencyDistribution {
            operations,
            elapsed_nanos,
            operations_per_second: throughput(operations, elapsed_nanos)?,
            p50_nanos: self.quantile(operations, 50)?,
            p95_nanos: self.quantile(operations, 95)?,
            p99_nanos: self.quantile(operations, 99)?,
            maximum_nanos: self.maximum.load(Ordering::Acquire),
        })
    }

    fn quantile(&self, count: u64, percentile: u64) -> Result<u64> {
        let target = count
            .checked_mul(percentile)
            .and_then(|value| value.checked_add(99))
            .map(|value| value / 100)
            .context("release benchmark quantile rank overflowed")?;
        let mut cumulative = 0_u64;
        for (index, bucket) in self.buckets.iter().enumerate() {
            cumulative = cumulative
                .checked_add(bucket.load(Ordering::Acquire))
                .context("release benchmark histogram count overflowed")?;
            if cumulative >= target {
                return u64::try_from(index)
                    .context("release benchmark bucket index exceeds u64")?
                    .checked_add(1)
                    .and_then(|value| value.checked_mul(HISTOGRAM_BUCKET_NANOS))
                    .context("release benchmark quantile value overflowed");
            }
        }
        bail!("release benchmark histogram did not contain its declared observations")
    }
}

fn throughput(operations: u64, elapsed_nanos: u64) -> Result<u64> {
    let value = u128::from(operations)
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_div(u128::from(elapsed_nanos)))
        .context("release benchmark throughput calculation overflowed")?;
    u64::try_from(value).context("release benchmark throughput exceeds u64")
}

fn disposition_code(disposition: ActionHookDisposition) -> u8 {
    match disposition {
        ActionHookDisposition::NoAction => 1,
        ActionHookDisposition::Suppressed => 2,
        ActionHookDisposition::Dispatched => 3,
        ActionHookDisposition::Failed => 4,
    }
}
