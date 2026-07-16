//! Exact-generation pre-binding and nonblocking count-and-byte ingress.

use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;
use std::time::Duration;

use market_squawk_domain::Timestamp;
use market_squawk_sources::{CurrentDecodedProviderBatch, CurrentSourceAuthorityLease};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::system_timestamp;
use crate::authority::{RuntimeLease, ShardLease};
use crate::processor::GenerationAdmission;
use crate::{ShardId, ShardKey};

const COMMAND_SHARED_ALLOCATION_CHARGE: usize = 256;

/// One bounded best-effort diagnostic emitted outside the authority transition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LiveRuntimeHealthEvent {
    kind: LiveRuntimeHealthKind,
    shard: ShardId,
    route: Option<ShardKey>,
    observed_at: Timestamp,
}

impl LiveRuntimeHealthEvent {
    pub(crate) const fn new(
        kind: LiveRuntimeHealthKind,
        shard: ShardId,
        route: Option<ShardKey>,
        observed_at: Timestamp,
    ) -> Self {
        Self {
            kind,
            shard,
            route,
            observed_at,
        }
    }

    pub const fn kind(&self) -> LiveRuntimeHealthKind {
        self.kind
    }
    pub const fn shard(&self) -> ShardId {
        self.shard
    }
    pub const fn route(&self) -> Option<&ShardKey> {
        self.route.as_ref()
    }
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }
}

/// Closed diagnostic categories. These values never grant or restore authority.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveRuntimeHealthKind {
    ShardReady,
    IngressRejected,
    GenerationRejected,
    ProcessingRejected,
    SnapshotNotificationDropped,
    ShardExited,
}

#[derive(Debug)]
pub(crate) struct ShardCommand {
    pub(crate) batch: CurrentDecodedProviderBatch,
    pub(crate) admission: GenerationAdmission,
    pub(crate) retained_bytes: u32,
    pub(crate) _byte_permit: OwnedSemaphorePermit,
}

impl ShardCommand {
    fn checked_retained_bytes(
        batch: &CurrentDecodedProviderBatch,
        admission: &GenerationAdmission,
    ) -> Result<u32, LiveIngressError> {
        let admission_bytes = admission
            .retained_bytes()
            .map_err(|_| LiveIngressError::RetainedSizeOverflow)?;
        let retained = batch
            .retained_bytes()
            .checked_add(size_of::<Self>())
            .and_then(|value| value.checked_add(admission_bytes))
            .and_then(|value| value.checked_add(COMMAND_SHARED_ALLOCATION_CHARGE))
            .ok_or(LiveIngressError::RetainedSizeOverflow)?;
        u32::try_from(retained).map_err(|_| LiveIngressError::RetainedSizeOverflow)
    }
}

#[derive(Debug)]
pub(crate) struct RegistrationCommand {
    pub(crate) route: ShardKey,
    pub(crate) source: CurrentSourceAuthorityLease,
    pub(crate) response: oneshot::Sender<Result<GenerationAdmission, RegistrationFailure>>,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RegistrationFailure {
    #[error("source generation is not current for this route")]
    NotCurrent,
    #[error("route generation capacity is exhausted")]
    Capacity,
    #[error("route is not configured on this shard")]
    UnknownRoute,
}

#[derive(Clone, Debug)]
pub(crate) struct RouteIngressChannels {
    pub(crate) shard: ShardId,
    pub(crate) runtime: RuntimeLease,
    pub(crate) shard_liveness: ShardLease,
    pub(crate) mailbox: mpsc::Sender<ShardCommand>,
    pub(crate) byte_budget: Arc<Semaphore>,
    pub(crate) registration: mpsc::Sender<RegistrationCommand>,
    pub(crate) registration_deadline: Duration,
    pub(crate) maximum_message_bytes: u32,
    pub(crate) health: mpsc::Sender<LiveRuntimeHealthEvent>,
}

/// Runtime-wide pre-feed binding facade. It intentionally has no unbound publish method.
#[derive(Clone, Debug)]
pub struct LiveRuntimeIngress {
    pub(crate) routes: Arc<HashMap<ShardKey, RouteIngressChannels>>,
    pub(crate) runtime: RuntimeLease,
}

impl LiveRuntimeIngress {
    /// Binds one exact current Task 5 source generation before its producer starts feeding data.
    pub async fn bind_generation(
        &self,
        route: ShardKey,
        source: CurrentSourceAuthorityLease,
        cancellation: CancellationToken,
    ) -> Result<BoundShardIngress, LiveIngressBindError> {
        self.runtime
            .validate()
            .map_err(|_| LiveIngressBindError::RuntimeClosed)?;
        let channels = self
            .routes
            .get(&route)
            .cloned()
            .ok_or(LiveIngressBindError::UnknownRoute)?;
        channels
            .shard_liveness
            .validate()
            .map_err(|_| LiveIngressBindError::ShardClosed)?;
        let now = system_timestamp().map_err(|_| LiveIngressBindError::ClockRange)?;
        source
            .validate_at(now)
            .map_err(|_| LiveIngressBindError::SourceNotCurrent)?;

        let (response, receiver) = oneshot::channel();
        let command = RegistrationCommand {
            route: route.clone(),
            source,
            response,
        };
        let deadline = Instant::now()
            .checked_add(channels.registration_deadline)
            .ok_or(LiveIngressBindError::DeadlineRange)?;
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(LiveIngressBindError::Cancelled),
            () = tokio::time::sleep_until(deadline) => {
                return Err(LiveIngressBindError::DeadlineExceeded);
            }
            result = channels.registration.send(command) => {
                result.map_err(|_| LiveIngressBindError::ControlClosed)?;
            }
        }
        let admission = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(LiveIngressBindError::Cancelled),
            () = tokio::time::sleep_until(deadline) => {
                return Err(LiveIngressBindError::DeadlineExceeded);
            }
            result = receiver => {
                result
                    .map_err(|_| LiveIngressBindError::ControlClosed)?
                    .map_err(LiveIngressBindError::Registration)?
            }
        };
        Ok(BoundShardIngress {
            route,
            shard: channels.shard,
            runtime: channels.runtime,
            shard_liveness: channels.shard_liveness,
            mailbox: channels.mailbox,
            byte_budget: channels.byte_budget,
            maximum_message_bytes: channels.maximum_message_bytes,
            admission,
            health: channels.health,
        })
    }
}

/// Nonblocking producer handle bound to one route and one exact generation allocation.
#[derive(Clone, Debug)]
pub struct BoundShardIngress {
    route: ShardKey,
    shard: ShardId,
    runtime: RuntimeLease,
    shard_liveness: ShardLease,
    mailbox: mpsc::Sender<ShardCommand>,
    byte_budget: Arc<Semaphore>,
    maximum_message_bytes: u32,
    admission: GenerationAdmission,
    health: mpsc::Sender<LiveRuntimeHealthEvent>,
}

impl BoundShardIngress {
    /// Attempts exact count-and-byte admission without awaiting mailbox capacity.
    pub fn try_publish(&self, batch: CurrentDecodedProviderBatch) -> Result<(), LiveIngressError> {
        match self.try_publish_inner(batch) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.admission.invalidate_on_admission_failure();
                self.emit_rejection();
                Err(error)
            }
        }
    }

    fn try_publish_inner(
        &self,
        batch: CurrentDecodedProviderBatch,
    ) -> Result<(), LiveIngressError> {
        self.runtime
            .validate()
            .map_err(|_| LiveIngressError::RuntimeClosed)?;
        self.shard_liveness
            .validate()
            .map_err(|_| LiveIngressError::ShardClosed)?;
        let now = system_timestamp().map_err(|_| LiveIngressError::ClockRange)?;
        self.admission
            .validate_at(now)
            .map_err(|_| LiveIngressError::GenerationNotCurrent)?;
        if batch.key().venue() != self.route.venue()
            || batch.key().instrument() != self.route.instrument()
        {
            return Err(LiveIngressError::WrongRoute);
        }
        if !batch
            .current_lease()
            .binding()
            .shares_allocation_with(self.admission.source().binding())
        {
            return Err(LiveIngressError::SourceLeaseTransplant);
        }
        batch
            .validate_at(now)
            .map_err(|_| LiveIngressError::GenerationNotCurrent)?;
        let retained_bytes = ShardCommand::checked_retained_bytes(&batch, &self.admission)?;
        if retained_bytes > self.maximum_message_bytes {
            return Err(LiveIngressError::MessageTooLarge {
                retained: retained_bytes,
                maximum: self.maximum_message_bytes,
            });
        }
        let permit = Arc::clone(&self.byte_budget)
            .try_acquire_many_owned(retained_bytes)
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => LiveIngressError::ByteCapacityFull,
                tokio::sync::TryAcquireError::Closed => LiveIngressError::MailboxClosed,
            })?;
        let command = ShardCommand {
            batch,
            admission: self.admission.clone(),
            retained_bytes,
            _byte_permit: permit,
        };
        self.mailbox.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => LiveIngressError::CountCapacityFull,
            mpsc::error::TrySendError::Closed(_) => LiveIngressError::MailboxClosed,
        })
    }

    fn emit_rejection(&self) {
        let Ok(observed_at) = system_timestamp() else {
            return;
        };
        let _ = self.health.try_send(LiveRuntimeHealthEvent::new(
            LiveRuntimeHealthKind::IngressRejected,
            self.shard,
            Some(self.route.clone()),
            observed_at,
        ));
    }

    /// Returns the exact route permanently bound into this producer handle.
    pub const fn route(&self) -> &ShardKey {
        &self.route
    }

    /// Returns the owning shard identity.
    pub const fn shard(&self) -> ShardId {
        self.shard
    }
}

/// Pre-feed generation binding failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LiveIngressBindError {
    #[error("runtime incarnation is closed")]
    RuntimeClosed,
    #[error("route shard is closed")]
    ShardClosed,
    #[error("route is not configured")]
    UnknownRoute,
    #[error("source authority is not current")]
    SourceNotCurrent,
    #[error("generation registration was cancelled")]
    Cancelled,
    #[error("generation registration exceeded its bounded deadline")]
    DeadlineExceeded,
    #[error("generation registration deadline cannot be represented")]
    DeadlineRange,
    #[error("generation registration channel is closed")]
    ControlClosed,
    #[error("trusted system clock is outside the supported range")]
    ClockRange,
    #[error(transparent)]
    Registration(RegistrationFailure),
}

/// Nonblocking live mailbox admission failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LiveIngressError {
    #[error("runtime incarnation is closed")]
    RuntimeClosed,
    #[error("route shard is closed")]
    ShardClosed,
    #[error("bound source generation is no longer current")]
    GenerationNotCurrent,
    #[error("batch route differs from the bound venue/instrument route")]
    WrongRoute,
    #[error("batch source lease does not share the bound source allocation")]
    SourceLeaseTransplant,
    #[error("private command retained-byte accounting overflowed")]
    RetainedSizeOverflow,
    #[error("command retained bytes {retained} exceed per-message maximum {maximum}")]
    MessageTooLarge { retained: u32, maximum: u32 },
    #[error("shard mailbox count capacity is full")]
    CountCapacityFull,
    #[error("shard mailbox byte capacity is full")]
    ByteCapacityFull,
    #[error("shard mailbox is closed")]
    MailboxClosed,
    #[error("trusted system clock is outside the supported range")]
    ClockRange,
}
