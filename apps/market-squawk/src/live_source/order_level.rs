//! Generation-owned order-level actors and their shared bounded read directory.
//!
//! Every exact `(source, venue, instrument, generation)` route owns one [`OrderLevelBook`] inside
//! one task. Producers can enqueue canonical order-level batches, but they never receive a mutable
//! reference to the book and no mutex wraps it. Desktop, CLI, and MCP readers share this directory
//! and receive only bounded owned order snapshots or the explicitly derived price-level
//! projection. This module never converts order-level state into a `MarketEvent`.

use std::{
    mem::size_of,
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
    time::Instant,
};

use market_squawk_domain::{
    ChecksumTarget, ConnectionGeneration, DataQuality, InstrumentId, SourceId, SourceIdentifier,
    Timestamp, VenueId,
};
use market_squawk_live::{
    DepthLimit, OrderLevelBatch, OrderLevelBatchPayload, OrderLevelBook, OrderLevelBookError,
    OrderLevelEntry, OrderLevelLimits, OrderLevelPhase, OrderLevelPriceProjection,
    OrderLevelProjectionError, OrderLevelQuarantineReason, OrderLevelRoute, PriceLevelProjection,
};
use market_squawk_sources::MarketFreshness;
use thiserror::Error;
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::{JoinError, JoinHandle},
};
use tokio_util::sync::CancellationToken;

/// Code-owned ceiling for concurrently registered order-level generations.
pub(crate) const MAX_ORDER_LEVEL_DIRECTORY_BOOKS: usize = 4_096;

/// Code-owned ceiling for retained ingress commands per order-level actor.
pub(crate) const MAX_ORDER_LEVEL_INGRESS_COMMANDS: usize = 4_096;

/// Code-owned ceiling for queued or outstanding read leases per order-level actor.
pub(crate) const MAX_ORDER_LEVEL_OUTSTANDING_READS: usize = 1_024;

/// Exact directory identity. A provider symbol is route evidence, not part of this lookup key.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct OrderLevelBookKey {
    source_id: SourceId,
    venue_id: VenueId,
    instrument_id: InstrumentId,
    generation: ConnectionGeneration,
}

impl OrderLevelBookKey {
    /// Constructs an owned exact generation key.
    pub(crate) const fn new(
        source_id: SourceId,
        venue_id: VenueId,
        instrument_id: InstrumentId,
        generation: ConnectionGeneration,
    ) -> Self {
        Self {
            source_id,
            venue_id,
            instrument_id,
            generation,
        }
    }

    /// Fallibly owns a lookup key copied from a retained live snapshot.
    pub(crate) fn try_from_snapshot(
        source_id: &SourceId,
        venue_id: &VenueId,
        instrument_id: InstrumentId,
        generation: ConnectionGeneration,
    ) -> Result<Self, OrderLevelDirectoryError> {
        Ok(Self::new(
            try_clone_source_id(source_id)?,
            try_clone_venue_id(venue_id)?,
            instrument_id,
            generation,
        ))
    }

    /// Returns the provider/source identity.
    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact venue identity.
    pub(crate) const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the internal instrument identity.
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact connection generation.
    pub(crate) const fn generation(&self) -> ConnectionGeneration {
        self.generation
    }

    fn try_from_route(route: &OrderLevelRoute) -> Result<Self, OrderLevelDirectoryError> {
        Ok(Self::new(
            try_clone_source_id(route.source_id())?,
            try_clone_venue_id(route.venue_id())?,
            route.instrument_id(),
            route.generation(),
        ))
    }
}

/// Hard bounds shared by every actor registered in one directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OrderLevelActorLimits {
    ingress_commands: NonZeroUsize,
    ingress_bytes: NonZeroU32,
    ingress_order_units: NonZeroU32,
    outstanding_reads: NonZeroUsize,
    read_bytes: NonZeroU32,
    read_order_units: NonZeroU32,
}

impl OrderLevelActorLimits {
    /// Constructs exact queue, retained-byte, order-work, and read-result ceilings.
    pub(crate) fn try_new(
        ingress_commands: NonZeroUsize,
        ingress_bytes: NonZeroU32,
        ingress_order_units: NonZeroU32,
        outstanding_reads: NonZeroUsize,
        read_bytes: NonZeroU32,
        read_order_units: NonZeroU32,
    ) -> Result<Self, OrderLevelConfigurationError> {
        if ingress_commands.get() > MAX_ORDER_LEVEL_INGRESS_COMMANDS {
            return Err(OrderLevelConfigurationError::IngressCommands {
                requested: ingress_commands.get(),
                maximum: MAX_ORDER_LEVEL_INGRESS_COMMANDS,
            });
        }
        if outstanding_reads.get() > MAX_ORDER_LEVEL_OUTSTANDING_READS {
            return Err(OrderLevelConfigurationError::OutstandingReads {
                requested: outstanding_reads.get(),
                maximum: MAX_ORDER_LEVEL_OUTSTANDING_READS,
            });
        }
        for permits in [
            ingress_bytes.get() as usize,
            ingress_order_units.get() as usize,
            read_bytes.get() as usize,
            read_order_units.get() as usize,
        ] {
            if permits > Semaphore::MAX_PERMITS {
                return Err(OrderLevelConfigurationError::SemaphorePermits {
                    requested: permits,
                    maximum: Semaphore::MAX_PERMITS,
                });
            }
        }
        Ok(Self {
            ingress_commands,
            ingress_bytes,
            ingress_order_units,
            outstanding_reads,
            read_bytes,
            read_order_units,
        })
    }

    pub(crate) const fn ingress_commands(self) -> NonZeroUsize {
        self.ingress_commands
    }

    pub(crate) const fn ingress_bytes(self) -> NonZeroU32 {
        self.ingress_bytes
    }

    pub(crate) const fn ingress_order_units(self) -> NonZeroU32 {
        self.ingress_order_units
    }

    pub(crate) const fn outstanding_reads(self) -> NonZeroUsize {
        self.outstanding_reads
    }

    pub(crate) const fn read_bytes(self) -> NonZeroU32 {
        self.read_bytes
    }

    pub(crate) const fn read_order_units(self) -> NonZeroU32 {
        self.read_order_units
    }
}

/// Invalid code-owned directory or actor bounds.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum OrderLevelConfigurationError {
    /// The directory could retain too many generation owners.
    #[error("order-level directory requested {requested} books; maximum is {maximum}")]
    DirectoryBooks { requested: usize, maximum: usize },
    /// One actor could retain too many ingress commands.
    #[error("order-level actor requested {requested} ingress commands; maximum is {maximum}")]
    IngressCommands { requested: usize, maximum: usize },
    /// One actor could retain too many outstanding reads.
    #[error("order-level actor requested {requested} outstanding reads; maximum is {maximum}")]
    OutstandingReads { requested: usize, maximum: usize },
    /// A byte or order-unit budget exceeded the runtime semaphore representation.
    #[error("order-level actor requested {requested} permits; maximum is {maximum}")]
    SemaphorePermits { requested: usize, maximum: usize },
}

/// Stable terminal cause sent to the source supervisor exactly once.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum OrderLevelTerminalFailure {
    /// A producer could not publish within its monotonic deadline.
    #[error("order-level ingress deadline elapsed")]
    IngressDeadline,
    /// Checked retained-byte or order-work accounting overflowed its representation.
    #[error("order-level ingress accounting overflowed")]
    IngressAccountingOverflow,
    /// The count-bounded ingress was saturated.
    #[error("order-level ingress command capacity was exhausted")]
    IngressCountSaturated,
    /// The byte-bounded ingress was saturated.
    #[error("order-level ingress retained-byte capacity was exhausted")]
    IngressBytesSaturated,
    /// The order/work-bounded ingress was saturated.
    #[error("order-level ingress order-unit capacity was exhausted")]
    IngressOrderUnitsSaturated,
    /// The canonical book rejected an integrity, lifecycle, arithmetic, or resource invariant.
    #[error("canonical order-level book failed: {0}")]
    Book(#[source] OrderLevelBookError),
    /// A canonical price projection failed after book admission.
    #[error("canonical order-level price projection failed: {0}")]
    Projection(#[source] OrderLevelProjectionError),
    /// A bounded owned read exceeded its pre-admitted accounting envelope.
    #[error("order-level owned-read accounting overflowed")]
    ReadAccountingOverflow,
    /// The upstream source explicitly isolated this generation.
    #[error("upstream order-level generation was quarantined: {0:?}")]
    UpstreamQuarantine(OrderLevelQuarantineReason),
}

impl OrderLevelTerminalFailure {
    const fn quarantine_reason(self) -> OrderLevelQuarantineReason {
        match self {
            Self::Book(_) => OrderLevelQuarantineReason::Mutation,
            Self::UpstreamQuarantine(reason) => reason,
            Self::IngressDeadline
            | Self::IngressAccountingOverflow
            | Self::IngressCountSaturated
            | Self::IngressBytesSaturated
            | Self::IngressOrderUnitsSaturated
            | Self::Projection(_)
            | Self::ReadAccountingOverflow => OrderLevelQuarantineReason::Resource,
        }
    }
}

/// Nonblocking publisher for the sole actor writer.
#[derive(Debug)]
pub(crate) struct OrderLevelIngress {
    key: Arc<OrderLevelBookKey>,
    commands: mpsc::Sender<IngressCommand>,
    command_budget: Arc<Semaphore>,
    byte_budget: Arc<Semaphore>,
    order_budget: Arc<Semaphore>,
    terminal_requests: watch::Sender<Option<OrderLevelTerminalFailure>>,
    terminal_state: watch::Receiver<Option<OrderLevelTerminalFailure>>,
    actor_status: watch::Receiver<Option<OrderLevelTerminalFailure>>,
    actor_cancellation: CancellationToken,
}

impl OrderLevelIngress {
    /// Returns the exact generation receiving every submitted batch.
    pub(crate) fn key(&self) -> &OrderLevelBookKey {
        &self.key
    }

    /// Enqueues one canonical transaction without waiting or dropping an older transaction.
    ///
    /// Any deadline, accounting, queue-count, byte, or order-unit overflow terminally isolates
    /// this generation. The actor observes the terminal request before another queued mutation,
    /// quarantines its owned book, and signals the supervisor.
    pub(crate) fn try_publish(
        &self,
        batch: OrderLevelBatch,
        deadline: Instant,
    ) -> Result<(), OrderLevelIngressError> {
        self.require_open()?;
        if Instant::now() >= deadline {
            return Err(self.fail(OrderLevelTerminalFailure::IngressDeadline));
        }
        let retained_bytes = batch
            .retained_bytes()
            .ok()
            .and_then(|bytes| bytes.checked_add(size_of::<IngressTicket>()))
            .and_then(|bytes| u32::try_from(bytes).ok())
            .ok_or_else(|| self.fail(OrderLevelTerminalFailure::IngressAccountingOverflow))?;
        let order_units = batch_order_units(&batch)
            .and_then(|units| u32::try_from(units).ok())
            .ok_or_else(|| self.fail(OrderLevelTerminalFailure::IngressAccountingOverflow))?;

        let command_permit = Arc::clone(&self.command_budget)
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => {
                    self.fail(OrderLevelTerminalFailure::IngressCountSaturated)
                }
                tokio::sync::TryAcquireError::Closed => OrderLevelIngressError::WorkerClosed,
            })?;
        let byte_permit = Arc::clone(&self.byte_budget)
            .try_acquire_many_owned(retained_bytes)
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => {
                    self.fail(OrderLevelTerminalFailure::IngressBytesSaturated)
                }
                tokio::sync::TryAcquireError::Closed => OrderLevelIngressError::WorkerClosed,
            })?;
        let order_permit = Arc::clone(&self.order_budget)
            .try_acquire_many_owned(order_units)
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => {
                    self.fail(OrderLevelTerminalFailure::IngressOrderUnitsSaturated)
                }
                tokio::sync::TryAcquireError::Closed => OrderLevelIngressError::WorkerClosed,
            })?;
        let command = IngressCommand {
            batch,
            _ticket: IngressTicket {
                _command_permit: command_permit,
                _byte_permit: byte_permit,
                _order_permit: order_permit,
            },
        };
        match self.commands.try_send(command) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_command)) => {
                Err(self.fail(OrderLevelTerminalFailure::IngressCountSaturated))
            }
            Err(mpsc::error::TrySendError::Closed(_command)) => {
                self.require_open()?;
                Err(OrderLevelIngressError::WorkerClosed)
            }
        }
    }

    /// Requests fail-closed isolation for a decoder or transport failure upstream of a batch.
    pub(crate) fn request_quarantine(
        &self,
        reason: OrderLevelQuarantineReason,
        deadline: Instant,
    ) -> Result<(), OrderLevelIngressError> {
        self.require_open()?;
        if Instant::now() >= deadline {
            return Err(self.fail(OrderLevelTerminalFailure::IngressDeadline));
        }
        let failure = OrderLevelTerminalFailure::UpstreamQuarantine(reason);
        self.request_terminal(failure);
        Ok(())
    }

    fn require_open(&self) -> Result<(), OrderLevelIngressError> {
        if let Some(failure) = *self.terminal_state.borrow() {
            return Err(OrderLevelIngressError::Terminal(failure));
        }
        if let Some(failure) = *self.actor_status.borrow() {
            return Err(OrderLevelIngressError::Terminal(failure));
        }
        if self.actor_cancellation.is_cancelled()
            || self.commands.is_closed()
            || self.terminal_requests.is_closed()
        {
            return Err(OrderLevelIngressError::WorkerClosed);
        }
        Ok(())
    }

    fn fail(&self, failure: OrderLevelTerminalFailure) -> OrderLevelIngressError {
        self.request_terminal(failure);
        OrderLevelIngressError::Terminal(failure)
    }

    fn request_terminal(&self, failure: OrderLevelTerminalFailure) {
        self.terminal_requests.send_if_modified(|current| {
            if current.is_none() {
                *current = Some(failure);
                true
            } else {
                false
            }
        });
    }
}

/// A live producer could not enqueue into the exact generation actor.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum OrderLevelIngressError {
    /// The generation has already entered a terminal fail-closed state.
    #[error("order-level generation failed terminally: {0}")]
    Terminal(#[source] OrderLevelTerminalFailure),
    /// Unregistration, shutdown, or unexpected actor exit closed ingress.
    #[error("order-level generation actor is closed")]
    WorkerClosed,
}

#[derive(Debug)]
struct IngressTicket {
    _command_permit: OwnedSemaphorePermit,
    _byte_permit: OwnedSemaphorePermit,
    _order_permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
struct IngressCommand {
    batch: OrderLevelBatch,
    _ticket: IngressTicket,
}

/// A bounded owned view of distinct provider orders.
///
/// The private budget ticket keeps count, bytes, and order units charged until every consumer is
/// finished with the view. The canonical book guarantees that identities in `orders` are unique.
#[derive(Debug)]
pub(crate) struct OrderLevelOrdersRead {
    revision: u64,
    phase: OrderLevelPhase,
    quality: DataQuality,
    freshness: MarketFreshness,
    available_at: Timestamp,
    total_order_count: usize,
    orders: Vec<OrderLevelEntry>,
    _ticket: ReadBudgetTicket,
}

impl OrderLevelOrdersRead {
    pub(crate) const fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) const fn phase(&self) -> OrderLevelPhase {
        self.phase
    }

    pub(crate) const fn quality(&self) -> DataQuality {
        self.quality
    }

    pub(crate) const fn freshness(&self) -> MarketFreshness {
        self.freshness
    }

    pub(crate) const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    pub(crate) const fn total_order_count(&self) -> usize {
        self.total_order_count
    }

    pub(crate) fn orders(&self) -> &[OrderLevelEntry] {
        &self.orders
    }

    pub(crate) const fn is_truncated(&self) -> bool {
        self.orders.len() < self.total_order_count
    }
}

/// A bounded lease around the canonical source-preserving price projection.
#[derive(Debug)]
pub(crate) struct OrderLevelPriceRead {
    projection: OrderLevelPriceProjection,
    _ticket: ReadBudgetTicket,
}

impl OrderLevelPriceRead {
    pub(crate) const fn projection(&self) -> &OrderLevelPriceProjection {
        &self.projection
    }
}

#[derive(Debug)]
struct ReadBudgetTicket {
    charged_bytes: u32,
    _count_permit: OwnedSemaphorePermit,
    _byte_permit: OwnedSemaphorePermit,
    _order_permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
enum ReadCommand {
    Orders {
        maximum_orders: usize,
        response: oneshot::Sender<Result<OrderLevelOrdersRead, OrderLevelReadError>>,
        ticket: ReadBudgetTicket,
    },
    PriceProjection {
        response: oneshot::Sender<Result<OrderLevelPriceRead, OrderLevelReadError>>,
        ticket: ReadBudgetTicket,
    },
}

/// A shared order-level read was rejected without publishing a partial result.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum OrderLevelReadError {
    /// No snapshot has initialized this generation.
    #[error("order-level generation is awaiting its first complete snapshot")]
    Unavailable,
    /// The exact generation is not registered.
    #[error("order-level generation is not registered")]
    NotRegistered,
    /// Exact-generation unregistration is already in progress.
    #[error("order-level generation is being unregistered")]
    Unregistering,
    /// The caller-provided order maximum exceeds this book's admitted maximum.
    #[error("order-level read requested {requested} orders; maximum is {maximum}")]
    OrderLimit { requested: usize, maximum: usize },
    /// Read-result byte or order-unit accounting overflowed.
    #[error("order-level read accounting overflowed")]
    AccountingOverflow,
    /// The requested result can never fit the configured read-byte budget.
    #[error("order-level read requires {requested} bytes; budget is {maximum}")]
    ByteLimit { requested: u32, maximum: u32 },
    /// The requested result can never fit the configured read-order budget.
    #[error("order-level read requires {requested} order units; budget is {maximum}")]
    OrderBudget { requested: u32, maximum: u32 },
    /// Caller cancellation won the bounded wait.
    #[error("order-level read was cancelled")]
    Cancelled,
    /// The monotonic read deadline elapsed.
    #[error("order-level read deadline elapsed")]
    Deadline,
    /// The actor was unregistered, shut down, or exited unexpectedly.
    #[error("order-level generation actor is closed")]
    WorkerClosed,
    /// The canonical bounded order clone failed.
    #[error("canonical order-level read failed: {0}")]
    Book(#[source] OrderLevelBookError),
    /// The canonical price projection failed.
    #[error("canonical order-level price projection failed: {0}")]
    Projection(#[source] OrderLevelProjectionError),
}

#[derive(Clone, Debug)]
struct ReadClient {
    commands: mpsc::Sender<ReadCommand>,
    count_budget: Arc<Semaphore>,
    byte_budget: Arc<Semaphore>,
    order_budget: Arc<Semaphore>,
    maximum_bytes: u32,
    maximum_order_units: u32,
    maximum_book_orders: usize,
    projection_depth: usize,
    actor_cancellation: CancellationToken,
}

impl ReadClient {
    async fn orders(
        &self,
        maximum_orders: NonZeroUsize,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<OrderLevelOrdersRead, OrderLevelReadError> {
        let maximum_orders = maximum_orders.get();
        if maximum_orders > self.maximum_book_orders {
            return Err(OrderLevelReadError::OrderLimit {
                requested: maximum_orders,
                maximum: self.maximum_book_orders,
            });
        }
        let bytes = order_read_charge(maximum_orders)?;
        let order_units = u32::try_from(maximum_orders)
            .map_err(|_error| OrderLevelReadError::AccountingOverflow)?;
        let ticket = self
            .reserve(bytes, order_units, cancellation, deadline)
            .await?;
        let (response, receiver) = oneshot::channel();
        self.send(
            ReadCommand::Orders {
                maximum_orders,
                response,
                ticket,
            },
            cancellation,
            deadline,
        )
        .await?;
        await_read_response(receiver, &self.actor_cancellation, cancellation, deadline).await
    }

    async fn price_projection(
        &self,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<OrderLevelPriceRead, OrderLevelReadError> {
        let bytes = projection_read_charge(self.projection_depth)?;
        let order_units = self
            .projection_depth
            .checked_mul(2)
            .and_then(|units| u32::try_from(units).ok())
            .ok_or(OrderLevelReadError::AccountingOverflow)?;
        let ticket = self
            .reserve(bytes, order_units, cancellation, deadline)
            .await?;
        let (response, receiver) = oneshot::channel();
        self.send(
            ReadCommand::PriceProjection { response, ticket },
            cancellation,
            deadline,
        )
        .await?;
        await_read_response(receiver, &self.actor_cancellation, cancellation, deadline).await
    }

    async fn reserve(
        &self,
        bytes: u32,
        order_units: u32,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<ReadBudgetTicket, OrderLevelReadError> {
        if bytes > self.maximum_bytes {
            return Err(OrderLevelReadError::ByteLimit {
                requested: bytes,
                maximum: self.maximum_bytes,
            });
        }
        if order_units > self.maximum_order_units {
            return Err(OrderLevelReadError::OrderBudget {
                requested: order_units,
                maximum: self.maximum_order_units,
            });
        }
        let count_permit = acquire_one(
            Arc::clone(&self.count_budget),
            &self.actor_cancellation,
            cancellation,
            deadline,
        )
        .await?;
        let byte_permit = acquire_many(
            Arc::clone(&self.byte_budget),
            bytes,
            &self.actor_cancellation,
            cancellation,
            deadline,
        )
        .await?;
        let order_permit = acquire_many(
            Arc::clone(&self.order_budget),
            order_units,
            &self.actor_cancellation,
            cancellation,
            deadline,
        )
        .await?;
        Ok(ReadBudgetTicket {
            charged_bytes: bytes,
            _count_permit: count_permit,
            _byte_permit: byte_permit,
            _order_permit: order_permit,
        })
    }

    async fn send(
        &self,
        command: ReadCommand,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<(), OrderLevelReadError> {
        require_read_time(cancellation, deadline)?;
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(OrderLevelReadError::Cancelled),
            () = self.actor_cancellation.cancelled() => Err(OrderLevelReadError::WorkerClosed),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                Err(OrderLevelReadError::Deadline)
            }
            result = self.commands.send(command) => {
                result.map_err(|_error| OrderLevelReadError::WorkerClosed)
            }
        }
    }
}

/// Supervisor-facing monitor for one exact generation's terminal status.
#[derive(Debug)]
pub(crate) struct OrderLevelSupervisorMonitor {
    status: watch::Receiver<Option<OrderLevelTerminalFailure>>,
}

impl OrderLevelSupervisorMonitor {
    /// Waits for terminal quarantine for the lifetime of one source generation.
    ///
    /// Source generation supervision is intentionally long-running. Cancellation is the bounded
    /// lifecycle owner here; request deadlines remain on registration, reads, and cleanup.
    pub(crate) async fn wait_until_terminal(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<OrderLevelTerminalFailure, OrderLevelMonitorError> {
        if let Some(failure) = *self.status.borrow_and_update() {
            return Ok(failure);
        }
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(OrderLevelMonitorError::Cancelled),
            result = self.status.changed() => match result {
                Ok(()) => (*self.status.borrow_and_update())
                    .ok_or(OrderLevelMonitorError::WorkerClosed),
                Err(_closed) => Err(OrderLevelMonitorError::WorkerClosed),
            }
        }
    }
}

/// Failure while awaiting one actor's supervisor signal.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum OrderLevelMonitorError {
    #[error("order-level supervisor wait was cancelled")]
    Cancelled,
    #[error("order-level actor exited without a terminal failure signal")]
    WorkerClosed,
}

/// Exact handles created by one successful generation registration.
#[derive(Debug)]
pub(crate) struct OrderLevelRegistration {
    ingress: OrderLevelIngress,
    monitor: OrderLevelSupervisorMonitor,
}

impl OrderLevelRegistration {
    pub(crate) fn key(&self) -> &OrderLevelBookKey {
        self.ingress.key()
    }

    pub(crate) fn into_parts(self) -> (OrderLevelIngress, OrderLevelSupervisorMonitor) {
        (self.ingress, self.monitor)
    }
}

/// Cloneable, process-local order-level read and exact-generation lifecycle directory.
#[derive(Clone, Debug)]
pub(crate) struct OrderLevelDirectory {
    inner: Arc<DirectoryInner>,
}

#[derive(Debug)]
struct DirectoryInner {
    maximum_books: usize,
    lifecycle: Mutex<()>,
    entries: Mutex<Vec<ActorEntry>>,
    cancellation: CancellationToken,
}

impl Drop for DirectoryInner {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

impl OrderLevelDirectory {
    /// Preallocates the complete bounded registry before it can be shared.
    pub(crate) fn try_new(
        maximum_books: NonZeroUsize,
        cancellation: CancellationToken,
    ) -> Result<Self, OrderLevelDirectoryError> {
        if maximum_books.get() > MAX_ORDER_LEVEL_DIRECTORY_BOOKS {
            return Err(OrderLevelDirectoryError::Configuration(
                OrderLevelConfigurationError::DirectoryBooks {
                    requested: maximum_books.get(),
                    maximum: MAX_ORDER_LEVEL_DIRECTORY_BOOKS,
                },
            ));
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(maximum_books.get())
            .map_err(|_error| OrderLevelDirectoryError::Allocation)?;
        Ok(Self {
            inner: Arc::new(DirectoryInner {
                maximum_books: maximum_books.get(),
                lifecycle: Mutex::new(()),
                entries: Mutex::new(entries),
                cancellation,
            }),
        })
    }

    /// Registers exactly one actor-owned canonical book for one generation.
    pub(crate) async fn register(
        &self,
        route: OrderLevelRoute,
        book_limits: OrderLevelLimits,
        actor_limits: OrderLevelActorLimits,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<OrderLevelRegistration, OrderLevelDirectoryError> {
        let _lifecycle = lock_directory(
            &self.inner.lifecycle,
            &self.inner.cancellation,
            cancellation,
            deadline,
        )
        .await?;
        if self.inner.cancellation.is_cancelled() {
            return Err(OrderLevelDirectoryError::Closed);
        }
        let key = Arc::new(OrderLevelBookKey::try_from_route(&route)?);
        let compact_route = try_compact_route(route)?;
        let book = OrderLevelBook::try_new(compact_route, book_limits)
            .map_err(OrderLevelDirectoryError::Book)?;
        let mut entries = lock_directory(
            &self.inner.entries,
            &self.inner.cancellation,
            cancellation,
            deadline,
        )
        .await?;
        let insertion = match entries.binary_search_by(|entry| entry.key.as_ref().cmp(key.as_ref()))
        {
            Ok(_index) => return Err(OrderLevelDirectoryError::AlreadyRegistered),
            Err(index) => index,
        };
        if entries.len() == self.inner.maximum_books {
            return Err(OrderLevelDirectoryError::Capacity);
        }
        if self.inner.cancellation.is_cancelled() {
            return Err(OrderLevelDirectoryError::Closed);
        }
        if cancellation.is_cancelled() {
            return Err(OrderLevelDirectoryError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(OrderLevelDirectoryError::Deadline);
        }

        let actor_cancellation = self.inner.cancellation.child_token();
        let command_budget = Arc::new(Semaphore::new(actor_limits.ingress_commands().get()));
        let byte_budget = Arc::new(Semaphore::new(actor_limits.ingress_bytes().get() as usize));
        let order_budget = Arc::new(Semaphore::new(
            actor_limits.ingress_order_units().get() as usize
        ));
        let read_count_budget = Arc::new(Semaphore::new(actor_limits.outstanding_reads().get()));
        let read_byte_budget = Arc::new(Semaphore::new(actor_limits.read_bytes().get() as usize));
        let read_order_budget = Arc::new(Semaphore::new(
            actor_limits.read_order_units().get() as usize
        ));
        let (command_sender, command_receiver) =
            mpsc::channel(actor_limits.ingress_commands().get());
        let (read_sender, read_receiver) = mpsc::channel(actor_limits.outstanding_reads().get());
        let (terminal_requests, terminal_request_receiver) = watch::channel(None);
        let terminal_state = terminal_requests.subscribe();
        let (status_sender, status) = watch::channel(None);
        let worker_cancellation = actor_cancellation.clone();
        let worker = tokio::spawn(run_actor(
            book,
            command_receiver,
            read_receiver,
            terminal_request_receiver,
            status_sender,
            worker_cancellation,
        ));
        let read_client = ReadClient {
            commands: read_sender,
            count_budget: read_count_budget,
            byte_budget: read_byte_budget,
            order_budget: read_order_budget,
            maximum_bytes: actor_limits.read_bytes().get(),
            maximum_order_units: actor_limits.read_order_units().get(),
            maximum_book_orders: book_limits.max_orders(),
            projection_depth: book_limits.price_level_depth().get(),
            actor_cancellation: actor_cancellation.clone(),
        };
        entries.insert(
            insertion,
            ActorEntry {
                key: Arc::clone(&key),
                read_client,
                cancellation: actor_cancellation.clone(),
                worker: Some(worker),
                unregistering: false,
            },
        );
        drop(entries);
        Ok(OrderLevelRegistration {
            ingress: OrderLevelIngress {
                key: Arc::clone(&key),
                commands: command_sender,
                command_budget,
                byte_budget,
                order_budget,
                terminal_requests,
                terminal_state,
                actor_status: status.clone(),
                actor_cancellation,
            },
            monitor: OrderLevelSupervisorMonitor { status },
        })
    }

    /// Reads a bounded prefix of the canonical, identity-distinct order set.
    pub(crate) async fn read_orders(
        &self,
        key: &OrderLevelBookKey,
        maximum_orders: NonZeroUsize,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<OrderLevelOrdersRead, OrderLevelReadError> {
        let reader = self.reader(key, cancellation, deadline).await?;
        reader.orders(maximum_orders, cancellation, deadline).await
    }

    /// Reads the canonical, source-preserving bounded price projection.
    pub(crate) async fn read_price_projection(
        &self,
        key: &OrderLevelBookKey,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<OrderLevelPriceRead, OrderLevelReadError> {
        let reader = self.reader(key, cancellation, deadline).await?;
        reader.price_projection(cancellation, deadline).await
    }

    /// Unregisters only the exact named generation and reaps its owned actor.
    pub(crate) async fn unregister(
        &self,
        key: &OrderLevelBookKey,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<OrderLevelActorShutdown, OrderLevelDirectoryError> {
        let cleanup_wait = CancellationToken::new();
        let _lifecycle = lock_directory(
            &self.inner.lifecycle,
            &cleanup_wait,
            &cleanup_wait,
            deadline,
        )
        .await?;
        let (worker, actor_cancellation) = {
            let mut entries =
                lock_directory(&self.inner.entries, &cleanup_wait, &cleanup_wait, deadline).await?;
            let index = entries
                .binary_search_by(|entry| entry.key.as_ref().cmp(key))
                .map_err(|_index| OrderLevelDirectoryError::NotRegistered)?;
            let entry = &mut entries[index];
            if entry.unregistering {
                return Err(OrderLevelDirectoryError::Unregistering);
            }
            entry.unregistering = true;
            let worker = entry
                .worker
                .take()
                .ok_or(OrderLevelDirectoryError::Unregistering)?;
            (worker, entry.cancellation.clone())
        };
        actor_cancellation.cancel();
        let outcome = stop_worker(worker, cancellation, deadline).await;
        let mut entries = self.inner.entries.lock().await;
        if let Ok(index) = entries.binary_search_by(|entry| entry.key.as_ref().cmp(key)) {
            entries.remove(index);
        }
        Ok(outcome)
    }

    /// Cancels and reaps every bounded actor under one absolute deadline.
    pub(crate) async fn shutdown(
        &self,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<OrderLevelDirectoryShutdown, OrderLevelDirectoryError> {
        let cleanup_wait = CancellationToken::new();
        let _lifecycle = lock_directory(
            &self.inner.lifecycle,
            &cleanup_wait,
            &cleanup_wait,
            deadline,
        )
        .await?;
        self.inner.cancellation.cancel();
        let mut owned = {
            let mut entries = self.inner.entries.lock().await;
            std::mem::take(&mut *entries)
        };
        let mut report = OrderLevelDirectoryShutdown::default();
        for entry in &mut owned {
            entry.cancellation.cancel();
            let Some(worker) = entry.worker.take() else {
                report.failed = report.failed.saturating_add(1);
                continue;
            };
            match stop_worker(worker, cancellation, deadline).await {
                OrderLevelActorShutdown::Graceful => {
                    report.graceful = report.graceful.saturating_add(1);
                }
                OrderLevelActorShutdown::AbortedAtDeadline => {
                    report.aborted_at_deadline = report.aborted_at_deadline.saturating_add(1);
                }
                OrderLevelActorShutdown::AbortedOnCancellation => {
                    report.aborted_on_cancellation =
                        report.aborted_on_cancellation.saturating_add(1);
                }
                OrderLevelActorShutdown::WorkerFailed => {
                    report.failed = report.failed.saturating_add(1);
                }
            }
        }
        Ok(report)
    }

    async fn reader(
        &self,
        key: &OrderLevelBookKey,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<ReadClient, OrderLevelReadError> {
        require_read_time(cancellation, deadline)?;
        if self.inner.cancellation.is_cancelled() {
            return Err(OrderLevelReadError::WorkerClosed);
        }
        let entries = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(OrderLevelReadError::Cancelled),
            () = self.inner.cancellation.cancelled() => {
                return Err(OrderLevelReadError::WorkerClosed);
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(OrderLevelReadError::Deadline);
            }
            entries = self.inner.entries.lock() => entries,
        };
        let index = entries
            .binary_search_by(|entry| entry.key.as_ref().cmp(key))
            .map_err(|_index| OrderLevelReadError::NotRegistered)?;
        let entry = &entries[index];
        if entry.unregistering {
            return Err(OrderLevelReadError::Unregistering);
        }
        Ok(entry.read_client.clone())
    }
}

/// Exact actor shutdown disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OrderLevelActorShutdown {
    Graceful,
    AbortedAtDeadline,
    AbortedOnCancellation,
    WorkerFailed,
}

/// Aggregate bounded-directory shutdown evidence.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct OrderLevelDirectoryShutdown {
    graceful: usize,
    aborted_at_deadline: usize,
    aborted_on_cancellation: usize,
    failed: usize,
}

impl OrderLevelDirectoryShutdown {
    pub(crate) const fn graceful(self) -> usize {
        self.graceful
    }

    pub(crate) const fn aborted_at_deadline(self) -> usize {
        self.aborted_at_deadline
    }

    pub(crate) const fn aborted_on_cancellation(self) -> usize {
        self.aborted_on_cancellation
    }

    pub(crate) const fn failed(self) -> usize {
        self.failed
    }

    pub(crate) const fn is_complete(self) -> bool {
        self.aborted_at_deadline == 0 && self.aborted_on_cancellation == 0 && self.failed == 0
    }
}

/// Exact-generation registration or lifecycle failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum OrderLevelDirectoryError {
    #[error("invalid order-level directory configuration: {0}")]
    Configuration(#[from] OrderLevelConfigurationError),
    #[error("order-level directory allocation failed")]
    Allocation,
    #[error("order-level directory identity invariant failed")]
    IdentityInvariant,
    #[error("order-level directory is closed")]
    Closed,
    #[error("order-level directory operation was cancelled")]
    Cancelled,
    #[error("order-level directory operation deadline elapsed")]
    Deadline,
    #[error("exact order-level generation is already registered")]
    AlreadyRegistered,
    #[error("exact order-level generation is not registered")]
    NotRegistered,
    #[error("exact order-level generation is already being unregistered")]
    Unregistering,
    #[error("order-level directory generation capacity was exhausted")]
    Capacity,
    #[error("canonical order-level book construction failed: {0}")]
    Book(#[source] OrderLevelBookError),
}

#[derive(Debug)]
struct ActorEntry {
    key: Arc<OrderLevelBookKey>,
    read_client: ReadClient,
    cancellation: CancellationToken,
    worker: Option<JoinHandle<()>>,
    unregistering: bool,
}

impl Drop for ActorEntry {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(worker) = &self.worker {
            worker.abort();
        }
    }
}

async fn run_actor(
    mut book: OrderLevelBook,
    mut ingress: mpsc::Receiver<IngressCommand>,
    mut reads: mpsc::Receiver<ReadCommand>,
    mut terminal_requests: watch::Receiver<Option<OrderLevelTerminalFailure>>,
    status: watch::Sender<Option<OrderLevelTerminalFailure>>,
    cancellation: CancellationToken,
) {
    let mut ingress_open = true;
    let mut reads_open = true;
    let mut terminal_requests_open = true;
    let mut terminal = false;
    loop {
        if !ingress_open && !reads_open {
            break;
        }
        tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            changed = terminal_requests.changed(), if terminal_requests_open && !terminal => {
                match changed {
                    Ok(()) => {
                        if let Some(failure) = *terminal_requests.borrow_and_update() {
                            enter_terminal(
                                &mut book,
                                failure,
                                &status,
                                &mut ingress,
                                &mut ingress_open,
                            );
                            terminal = true;
                        }
                    }
                    Err(_closed) => terminal_requests_open = false,
                }
            }
            command = ingress.recv(), if ingress_open && !terminal => match command {
                Some(IngressCommand { batch, _ticket }) => {
                    if let Err(error) = book.apply(batch) {
                        enter_terminal(
                            &mut book,
                            OrderLevelTerminalFailure::Book(error),
                            &status,
                            &mut ingress,
                            &mut ingress_open,
                        );
                        terminal = true;
                    }
                    drop(_ticket);
                }
                None => ingress_open = false,
            },
            command = reads.recv(), if reads_open => match command {
                Some(command) => {
                    if let Some(failure) = process_read(&mut book, command) {
                        enter_terminal(
                            &mut book,
                            failure,
                            &status,
                            &mut ingress,
                            &mut ingress_open,
                        );
                        terminal = true;
                    }
                }
                None => reads_open = false,
            },
        }
    }
}

fn enter_terminal(
    book: &mut OrderLevelBook,
    failure: OrderLevelTerminalFailure,
    status: &watch::Sender<Option<OrderLevelTerminalFailure>>,
    ingress: &mut mpsc::Receiver<IngressCommand>,
    ingress_open: &mut bool,
) {
    if !matches!(failure, OrderLevelTerminalFailure::Book(_)) {
        book.quarantine(failure.quarantine_reason());
    }
    status.send_replace(Some(failure));
    ingress.close();
    while let Ok(command) = ingress.try_recv() {
        drop(command);
    }
    *ingress_open = false;
}

fn process_read(
    book: &mut OrderLevelBook,
    command: ReadCommand,
) -> Option<OrderLevelTerminalFailure> {
    match command {
        ReadCommand::Orders {
            maximum_orders,
            response,
            ticket,
        } => {
            let result = owned_orders(book, maximum_orders, ticket);
            let failure = result
                .as_ref()
                .err()
                .filter(|error| matches!(error, OrderLevelReadError::AccountingOverflow))
                .map(|_error| OrderLevelTerminalFailure::ReadAccountingOverflow);
            let _ignored = response.send(result);
            failure
        }
        ReadCommand::PriceProjection { response, ticket } => {
            let result = price_projection(book, ticket);
            let failure = result.as_ref().err().and_then(|error| match error {
                OrderLevelReadError::Projection(
                    projection @ OrderLevelProjectionError::Allocation,
                )
                | OrderLevelReadError::Projection(
                    projection @ OrderLevelProjectionError::NumericOverflow,
                ) => Some(OrderLevelTerminalFailure::Projection(*projection)),
                OrderLevelReadError::AccountingOverflow => {
                    Some(OrderLevelTerminalFailure::ReadAccountingOverflow)
                }
                _ => None,
            });
            let _ignored = response.send(result);
            failure
        }
    }
}

fn owned_orders(
    book: &OrderLevelBook,
    maximum_orders: usize,
    ticket: ReadBudgetTicket,
) -> Result<OrderLevelOrdersRead, OrderLevelReadError> {
    if book.phase() == OrderLevelPhase::AwaitingSnapshot {
        return Err(OrderLevelReadError::Unavailable);
    }
    let available_at = book
        .available_at()
        .ok_or(OrderLevelReadError::Unavailable)?;
    let total_order_count = book.orders().len();
    let orders = book
        .try_owned_orders(maximum_orders)
        .map_err(OrderLevelReadError::Book)?;
    let retained = owned_order_read_retained_bytes(&orders, orders.capacity())
        .ok_or(OrderLevelReadError::AccountingOverflow)?;
    if retained > ticket.charged_bytes as usize {
        return Err(OrderLevelReadError::AccountingOverflow);
    }
    Ok(OrderLevelOrdersRead {
        revision: book.revision(),
        phase: book.phase(),
        quality: book.quality(),
        freshness: book.freshness(),
        available_at,
        total_order_count,
        orders,
        _ticket: ticket,
    })
}

fn price_projection(
    book: &OrderLevelBook,
    ticket: ReadBudgetTicket,
) -> Result<OrderLevelPriceRead, OrderLevelReadError> {
    let projection = book
        .project_price_levels()
        .map_err(OrderLevelReadError::Projection)?;
    let retained = price_projection_retained_bytes(&projection, book.limits().price_level_depth())
        .ok_or(OrderLevelReadError::AccountingOverflow)?;
    if retained > ticket.charged_bytes as usize {
        return Err(OrderLevelReadError::AccountingOverflow);
    }
    Ok(OrderLevelPriceRead {
        projection,
        _ticket: ticket,
    })
}

fn batch_order_units(batch: &OrderLevelBatch) -> Option<usize> {
    let snapshot_orders = match batch.payload() {
        OrderLevelBatchPayload::Snapshot { orders, .. } => orders.len(),
        OrderLevelBatchPayload::Update { .. } => 0,
    };
    snapshot_orders.checked_add(batch.operation_count())
}

fn order_read_charge(maximum_orders: usize) -> Result<u32, OrderLevelReadError> {
    let per_order = size_of::<OrderLevelEntry>()
        .checked_add(
            SourceIdentifier::MAX_LENGTH
                .checked_mul(2)
                .ok_or(OrderLevelReadError::AccountingOverflow)?,
        )
        .ok_or(OrderLevelReadError::AccountingOverflow)?;
    size_of::<OrderLevelOrdersRead>()
        .checked_add(
            maximum_orders
                .checked_mul(per_order)
                .ok_or(OrderLevelReadError::AccountingOverflow)?,
        )
        .and_then(|bytes| u32::try_from(bytes).ok())
        .ok_or(OrderLevelReadError::AccountingOverflow)
}

fn projection_read_charge(depth: usize) -> Result<u32, OrderLevelReadError> {
    let level_bytes = depth
        .checked_mul(2)
        .and_then(|levels| levels.checked_mul(size_of::<PriceLevelProjection>()))
        .ok_or(OrderLevelReadError::AccountingOverflow)?;
    let identifier_bytes = SourceId::MAX_LENGTH
        .checked_add(VenueId::MAX_LENGTH)
        .and_then(|bytes| {
            SourceIdentifier::MAX_LENGTH
                .checked_mul(5)
                .and_then(|identifiers| bytes.checked_add(identifiers))
        })
        .ok_or(OrderLevelReadError::AccountingOverflow)?;
    size_of::<OrderLevelPriceRead>()
        .checked_add(level_bytes)
        .and_then(|bytes| bytes.checked_add(identifier_bytes))
        .and_then(|bytes| u32::try_from(bytes).ok())
        .ok_or(OrderLevelReadError::AccountingOverflow)
}

fn owned_order_read_retained_bytes(
    orders: &[OrderLevelEntry],
    order_capacity: usize,
) -> Option<usize> {
    let dynamic = orders.iter().try_fold(0_usize, |total, order| {
        total
            .checked_add(order.order_id().retained_bytes())?
            .checked_add(
                order
                    .provider_priority()
                    .map_or(0, |priority| priority.rule().retained_bytes()),
            )
    })?;
    size_of::<OrderLevelOrdersRead>()
        .checked_add(order_capacity.checked_mul(size_of::<OrderLevelEntry>())?)?
        .checked_add(dynamic)
}

fn price_projection_retained_bytes(
    projection: &OrderLevelPriceProjection,
    maximum_depth: DepthLimit,
) -> Option<usize> {
    let route = projection.route();
    let route_bytes = route
        .source_id()
        .retained_bytes()
        .checked_add(route.venue_id().retained_bytes())?
        .checked_add(route.provider_instrument().retained_bytes())?;
    let sequence_rule_bytes = projection
        .sequence_evidence()
        .rule()
        .map_or(0, |rule| rule.provider_rule().retained_bytes());
    let checksum_rule_bytes = projection
        .checksum_evidence()
        .rule()
        .map_or(0, |rule| rule.provider_rule().retained_bytes());
    let checksum_scope_bytes =
        projection
            .checksum_evidence()
            .target()
            .map_or(0, |target| match target {
                ChecksumTarget::Book(scope) => scope.provider_scope().retained_bytes(),
                ChecksumTarget::Payload(scope) => scope.provider_scope().retained_bytes(),
            });
    let level_bytes = maximum_depth
        .get()
        .checked_mul(2)?
        .checked_mul(size_of::<PriceLevelProjection>())?;
    size_of::<OrderLevelPriceRead>()
        .checked_add(route_bytes)?
        .checked_add(projection.batch_identifier().retained_bytes())?
        .checked_add(sequence_rule_bytes)?
        .checked_add(checksum_rule_bytes)?
        .checked_add(checksum_scope_bytes)?
        .checked_add(level_bytes)
}

async fn acquire_one(
    budget: Arc<Semaphore>,
    actor_cancellation: &CancellationToken,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<OwnedSemaphorePermit, OrderLevelReadError> {
    require_read_time(cancellation, deadline)?;
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(OrderLevelReadError::Cancelled),
        () = actor_cancellation.cancelled() => Err(OrderLevelReadError::WorkerClosed),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(OrderLevelReadError::Deadline)
        }
        permit = budget.acquire_owned() => {
            permit.map_err(|_closed| OrderLevelReadError::WorkerClosed)
        }
    }
}

async fn acquire_many(
    budget: Arc<Semaphore>,
    permits: u32,
    actor_cancellation: &CancellationToken,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<OwnedSemaphorePermit, OrderLevelReadError> {
    require_read_time(cancellation, deadline)?;
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(OrderLevelReadError::Cancelled),
        () = actor_cancellation.cancelled() => Err(OrderLevelReadError::WorkerClosed),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(OrderLevelReadError::Deadline)
        }
        permit = budget.acquire_many_owned(permits) => {
            permit.map_err(|_closed| OrderLevelReadError::WorkerClosed)
        }
    }
}

async fn await_read_response<T>(
    receiver: oneshot::Receiver<Result<T, OrderLevelReadError>>,
    actor_cancellation: &CancellationToken,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<T, OrderLevelReadError> {
    require_read_time(cancellation, deadline)?;
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(OrderLevelReadError::Cancelled),
        () = actor_cancellation.cancelled() => Err(OrderLevelReadError::WorkerClosed),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(OrderLevelReadError::Deadline)
        }
        result = receiver => {
            result.map_err(|_closed| OrderLevelReadError::WorkerClosed)?
        }
    }
}

fn require_read_time(
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<(), OrderLevelReadError> {
    if cancellation.is_cancelled() {
        Err(OrderLevelReadError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(OrderLevelReadError::Deadline)
    } else {
        Ok(())
    }
}

async fn lock_directory<'a, T>(
    mutex: &'a Mutex<T>,
    directory_cancellation: &CancellationToken,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<tokio::sync::MutexGuard<'a, T>, OrderLevelDirectoryError> {
    if cancellation.is_cancelled() {
        return Err(OrderLevelDirectoryError::Cancelled);
    }
    if directory_cancellation.is_cancelled() {
        return Err(OrderLevelDirectoryError::Closed);
    }
    if Instant::now() >= deadline {
        return Err(OrderLevelDirectoryError::Deadline);
    }
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(OrderLevelDirectoryError::Cancelled),
        () = directory_cancellation.cancelled() => Err(OrderLevelDirectoryError::Closed),
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            Err(OrderLevelDirectoryError::Deadline)
        }
        guard = mutex.lock() => Ok(guard),
    }
}

async fn stop_worker(
    mut worker: JoinHandle<()>,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> OrderLevelActorShutdown {
    if cancellation.is_cancelled() {
        worker.abort();
        let _joined = worker.await;
        return OrderLevelActorShutdown::AbortedOnCancellation;
    }
    if Instant::now() >= deadline {
        worker.abort();
        let _joined = worker.await;
        return OrderLevelActorShutdown::AbortedAtDeadline;
    }
    tokio::select! {
        biased;
        () = cancellation.cancelled() => {
            worker.abort();
            let _joined = worker.await;
            OrderLevelActorShutdown::AbortedOnCancellation
        }
        () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
            worker.abort();
            let _joined = worker.await;
            OrderLevelActorShutdown::AbortedAtDeadline
        }
        result = &mut worker => classify_worker_join(result),
    }
}

fn classify_worker_join(result: Result<(), JoinError>) -> OrderLevelActorShutdown {
    match result {
        Ok(()) => OrderLevelActorShutdown::Graceful,
        Err(_error) => OrderLevelActorShutdown::WorkerFailed,
    }
}

fn try_clone_source_id(value: &SourceId) -> Result<SourceId, OrderLevelDirectoryError> {
    let value = try_clone_text(value.as_str())?;
    SourceId::try_from(value).map_err(|_error| OrderLevelDirectoryError::IdentityInvariant)
}

fn try_clone_venue_id(value: &VenueId) -> Result<VenueId, OrderLevelDirectoryError> {
    let value = try_clone_text(value.as_str())?;
    VenueId::try_from(value).map_err(|_error| OrderLevelDirectoryError::IdentityInvariant)
}

fn try_clone_source_identifier(
    value: &SourceIdentifier,
) -> Result<SourceIdentifier, OrderLevelDirectoryError> {
    let value = try_clone_text(value.as_str())?;
    SourceIdentifier::try_from(value).map_err(|_error| OrderLevelDirectoryError::IdentityInvariant)
}

fn try_compact_route(route: OrderLevelRoute) -> Result<OrderLevelRoute, OrderLevelDirectoryError> {
    Ok(OrderLevelRoute::new(
        try_clone_source_id(route.source_id())?,
        try_clone_venue_id(route.venue_id())?,
        route.instrument_id(),
        try_clone_source_identifier(route.provider_instrument())?,
        route.generation(),
    ))
}

fn try_clone_text(value: &str) -> Result<String, OrderLevelDirectoryError> {
    let mut clone = String::new();
    clone
        .try_reserve_exact(value.len())
        .map_err(|_error| OrderLevelDirectoryError::Allocation)?;
    clone.push_str(value);
    Ok(clone)
}
