//! Nonblocking bounded `tracing` admission and dedicated persistence worker.

use std::{
    collections::BTreeMap,
    fmt::{self, Write as _},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc::{Receiver, SyncSender, TrySendError, sync_channel},
    },
    thread::JoinHandle,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use market_squawk_domain::Timestamp;
use tracing::{Event, Subscriber, field::Visit};
use tracing_subscriber::{Layer, layer::Context};

use super::{LogDomain, LogSeverity, StructuredLogError, StructuredLogEvent, StructuredLogStore};

const MAXIMUM_QUEUE_CAPACITY: usize = 65_536;
const MAXIMUM_VISITOR_FIELDS: usize = 32;
const MAXIMUM_VISITOR_VALUE_BYTES: usize = 2 * 1024;

enum WorkerMessage {
    Event(StructuredLogEvent),
    Flush(SyncSender<()>),
    Shutdown(SyncSender<()>),
}

#[derive(Debug, Default)]
struct LogCounters {
    accepted: AtomicU64,
    persisted: AtomicU64,
    dropped_overflow: AtomicU64,
    rejected_unsafe: AtomicU64,
    write_failures: AtomicU64,
}

/// Exact nonblocking-ingress and persistence evidence at a flush/shutdown barrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StructuredLogDrainEvidence {
    pub accepted: u64,
    pub persisted: u64,
    pub dropped_overflow: u64,
    pub rejected_unsafe: u64,
    pub write_failures: u64,
}

impl StructuredLogDrainEvidence {
    fn capture(counters: &LogCounters) -> Self {
        Self {
            accepted: counters.accepted.load(Ordering::Acquire),
            persisted: counters.persisted.load(Ordering::Acquire),
            dropped_overflow: counters.dropped_overflow.load(Ordering::Acquire),
            rejected_unsafe: counters.rejected_unsafe.load(Ordering::Acquire),
            write_failures: counters.write_failures.load(Ordering::Acquire),
        }
    }
}

/// Cloneable nonblocking subscriber layer; event callbacks never perform disk I/O.
#[derive(Clone, Debug)]
pub struct StructuredLogLayer {
    sender: SyncSender<WorkerMessage>,
    counters: Arc<LogCounters>,
}

/// Control-plane flush handle sharing the same bounded queue and accounting.
#[derive(Clone, Debug)]
pub struct StructuredLogDrain {
    sender: SyncSender<WorkerMessage>,
    counters: Arc<LogCounters>,
}

/// Dedicated writer lifecycle. Call shutdown for exact terminal evidence.
#[derive(Debug)]
pub struct StructuredLogWorker {
    sender: SyncSender<WorkerMessage>,
    counters: Arc<LogCounters>,
    join: Option<JoinHandle<()>>,
}

impl StructuredLogLayer {
    /// Creates one bounded ingress queue and dedicated persistence worker.
    pub fn try_spawn(
        store: Arc<StructuredLogStore>,
        capacity: usize,
    ) -> Result<(Self, StructuredLogDrain, StructuredLogWorker), StructuredLogError> {
        if capacity == 0 || capacity > MAXIMUM_QUEUE_CAPACITY {
            return Err(StructuredLogError::InvalidQueueCapacity);
        }
        let (sender, receiver) = sync_channel(capacity);
        let counters = Arc::new(LogCounters::default());
        let worker_counters = Arc::clone(&counters);
        let join = std::thread::Builder::new()
            .name("market-squawk-structured-logs".to_owned())
            .spawn(move || run_worker(store, receiver, &worker_counters))
            .map_err(|_| StructuredLogError::WorkerUnavailable)?;
        Ok((
            Self {
                sender: sender.clone(),
                counters: Arc::clone(&counters),
            },
            StructuredLogDrain {
                sender: sender.clone(),
                counters: Arc::clone(&counters),
            },
            StructuredLogWorker {
                sender,
                counters,
                join: Some(join),
            },
        ))
    }

    fn admit(&self, event: StructuredLogEvent) {
        match self.sender.try_send(WorkerMessage::Event(event)) {
            Ok(()) => {}
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                self.counters
                    .dropped_overflow
                    .fetch_add(1, Ordering::Release);
            }
        }
    }
}

impl StructuredLogDrain {
    /// Waits only on the control plane until every previously admitted event is processed.
    pub fn flush(
        &self,
        timeout: Duration,
    ) -> Result<StructuredLogDrainEvidence, StructuredLogError> {
        if timeout.is_zero() {
            return Err(StructuredLogError::DrainDeadlineElapsed);
        }
        let (acknowledge, completion) = sync_channel(1);
        self.sender
            .try_send(WorkerMessage::Flush(acknowledge))
            .map_err(|_| StructuredLogError::QueueUnavailable)?;
        completion
            .recv_timeout(timeout)
            .map_err(|_| StructuredLogError::DrainDeadlineElapsed)?;
        Ok(StructuredLogDrainEvidence::capture(&self.counters))
    }
}

impl StructuredLogWorker {
    /// Drains prior events, terminates the writer, joins it, and returns final accounting.
    pub fn shutdown(
        &mut self,
        timeout: Duration,
    ) -> Result<StructuredLogDrainEvidence, StructuredLogError> {
        if timeout.is_zero() {
            return Err(StructuredLogError::DrainDeadlineElapsed);
        }
        let (acknowledge, completion) = sync_channel(1);
        self.sender
            .try_send(WorkerMessage::Shutdown(acknowledge))
            .map_err(|_| StructuredLogError::QueueUnavailable)?;
        completion
            .recv_timeout(timeout)
            .map_err(|_| StructuredLogError::DrainDeadlineElapsed)?;
        self.join
            .take()
            .ok_or(StructuredLogError::WorkerUnavailable)?
            .join()
            .map_err(|_| StructuredLogError::WorkerUnavailable)?;
        Ok(StructuredLogDrainEvidence::capture(&self.counters))
    }
}

impl<S> Layer<S> for StructuredLogLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let metadata = event.metadata();
        let mut visitor = BoundedVisitor::default();
        event.record(&mut visitor);
        if visitor.overflowed {
            self.counters
                .rejected_unsafe
                .fetch_add(1, Ordering::Release);
            return;
        }
        let message = visitor
            .message
            .take()
            .unwrap_or_else(|| metadata.name().to_owned());
        let operation = visitor
            .operation
            .take()
            .or_else(|| Some(metadata.name().to_owned()));
        match StructuredLogEvent::try_new(
            Timestamp::from_unix_nanos(0),
            severity(metadata.level()),
            domain(metadata.target()),
            operation,
            visitor.source_id.take(),
            visitor.job_id.take(),
            visitor.correlation_id.take(),
            message,
            visitor.fields,
        ) {
            Ok(event) => self.admit(event),
            Err(_) => {
                self.counters
                    .rejected_unsafe
                    .fetch_add(1, Ordering::Release);
            }
        }
    }
}

fn run_worker(
    store: Arc<StructuredLogStore>,
    receiver: Receiver<WorkerMessage>,
    counters: &LogCounters,
) {
    let mut latest_timestamp = i64::MIN;
    while let Ok(message) = receiver.recv() {
        match message {
            WorkerMessage::Event(mut event) => {
                counters.accepted.fetch_add(1, Ordering::Release);
                event.observed_at = monotonic_timestamp(&mut latest_timestamp);
                if store.append(event).is_ok() {
                    counters.persisted.fetch_add(1, Ordering::Release);
                } else {
                    counters.write_failures.fetch_add(1, Ordering::Release);
                }
            }
            WorkerMessage::Flush(acknowledge) => {
                let _ignored = acknowledge.try_send(());
            }
            WorkerMessage::Shutdown(acknowledge) => {
                let _ignored = acknowledge.try_send(());
                break;
            }
        }
    }
}

fn monotonic_timestamp(latest: &mut i64) -> Timestamp {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
        .unwrap_or(*latest);
    *latest = nanos.max(latest.saturating_add(1));
    Timestamp::from_unix_nanos(*latest)
}

fn severity(level: &tracing::Level) -> LogSeverity {
    match *level {
        tracing::Level::TRACE => LogSeverity::Trace,
        tracing::Level::DEBUG => LogSeverity::Debug,
        tracing::Level::INFO => LogSeverity::Info,
        tracing::Level::WARN => LogSeverity::Warn,
        tracing::Level::ERROR => LogSeverity::Error,
    }
}

fn domain(target: &str) -> LogDomain {
    if target.contains("source") || target.contains("adapter") {
        LogDomain::Source
    } else if target.contains("portfolio") {
        LogDomain::Portfolio
    } else if target.contains("model") {
        LogDomain::Model
    } else if target.contains("backtest") {
        LogDomain::Backtest
    } else if target.contains("execution") {
        LogDomain::Execution
    } else if target.contains("risk") {
        LogDomain::Risk
    } else if target.contains("fair_value") || target.contains("valuation") {
        LogDomain::FairValue
    } else if target.contains("mcp") {
        LogDomain::Mcp
    } else if target.contains("lifecycle") || target.contains("workspace") {
        LogDomain::Lifecycle
    } else if target.contains("research") || target.contains("data") {
        LogDomain::Research
    } else if target.contains("::market")
        || target.contains("market_data")
        || target.contains("::live")
        || target.contains("_live")
    {
        LogDomain::Market
    } else {
        LogDomain::Application
    }
}

#[derive(Default)]
struct BoundedVisitor {
    message: Option<String>,
    operation: Option<String>,
    source_id: Option<String>,
    job_id: Option<String>,
    correlation_id: Option<String>,
    fields: BTreeMap<String, String>,
    overflowed: bool,
}

impl BoundedVisitor {
    fn record_value(&mut self, field: &tracing::field::Field, value: String) {
        if value.len() > MAXIMUM_VISITOR_VALUE_BYTES {
            self.overflowed = true;
            return;
        }
        match field.name() {
            "message" => self.message = Some(value),
            "operation" => self.operation = Some(value),
            "source_id" => self.source_id = Some(value),
            "job_id" => self.job_id = Some(value),
            "correlation_id" => self.correlation_id = Some(value),
            name if self.fields.len() < MAXIMUM_VISITOR_FIELDS => {
                self.fields.insert(name.to_owned(), value);
            }
            _ => self.overflowed = true,
        }
    }
}

impl Visit for BoundedVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        let mut bounded = BoundedFormatter::default();
        let _ignored = write!(&mut bounded, "{value:?}");
        if bounded.overflowed {
            self.overflowed = true;
        } else {
            self.record_value(field, bounded.value);
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if value.len() > MAXIMUM_VISITOR_VALUE_BYTES {
            self.overflowed = true;
        } else {
            self.record_value(field, value.to_owned());
        }
    }
}

#[derive(Default)]
struct BoundedFormatter {
    value: String,
    overflowed: bool,
}

impl fmt::Write for BoundedFormatter {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let remaining = MAXIMUM_VISITOR_VALUE_BYTES.saturating_sub(self.value.len());
        if value.len() > remaining {
            self.overflowed = true;
            return Err(fmt::Error);
        }
        self.value.push_str(value);
        Ok(())
    }
}
