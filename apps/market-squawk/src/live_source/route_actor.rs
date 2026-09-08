//! Permanent bounded actor for one configured live route.

use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
};

use market_squawk_live::{
    BoundShardIngress, DormantRouteIngress, LiveIngressBindError, LiveIngressRevokeError,
    LiveRuntimeIngress, ShardKey,
};
use market_squawk_sources::CurrentDecodedProviderBatch;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::sink::{ProductionSinkFailure, RouteActivationFailure};

/// Exact count-and-byte ceilings for one route actor's retained commands.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RouteBufferLimits {
    command_count: NonZeroUsize,
    batch_bytes: NonZeroU32,
}

impl RouteBufferLimits {
    pub(super) const fn new(command_count: NonZeroUsize, batch_bytes: NonZeroU32) -> Self {
        Self {
            command_count,
            batch_bytes,
        }
    }
}

/// Nonblocking producer for one permanent, bounded route actor.
#[derive(Debug)]
pub(super) struct RouteActivationPublisher {
    route: ShardKey,
    commands: mpsc::Sender<RouteCommand>,
    command_budget: Arc<Semaphore>,
    byte_budget: Arc<Semaphore>,
    status: watch::Receiver<Option<RouteActivationFailure>>,
    initial_activation_available: bool,
}

pub(super) type RouteActorWorker = tokio::task::JoinHandle<Result<(), RouteActivationFailure>>;

impl RouteActivationPublisher {
    pub(super) const fn route(&self) -> &ShardKey {
        &self.route
    }

    pub(super) fn prepare(
        &mut self,
        ingress: &LiveRuntimeIngress,
    ) -> Result<RouteActivationBinding, ProductionSinkFailure> {
        self.check_failure()?;
        if self.initial_activation_available {
            Ok(RouteActivationBinding::Initial)
        } else {
            ingress
                .reserve_route(self.route.clone())
                .map(RouteActivationBinding::Replacement)
                .map_err(|error| {
                    ProductionSinkFailure::RouteActivation(RouteActivationFailure::Bind(error))
                })
        }
    }

    pub(super) fn start_activation(
        &mut self,
        binding: RouteActivationBinding,
        batch: CurrentDecodedProviderBatch,
    ) -> Result<(), ProductionSinkFailure> {
        let initial = matches!(binding, RouteActivationBinding::Initial);
        let dormant = match binding {
            RouteActivationBinding::Initial => None,
            RouteActivationBinding::Replacement(dormant) => Some(dormant),
        };
        self.try_send(RouteCommandKind::Activate { dormant, batch })?;
        if initial {
            self.initial_activation_available = false;
        }
        Ok(())
    }

    pub(super) fn try_publish(
        &mut self,
        batch: CurrentDecodedProviderBatch,
    ) -> Result<(), ProductionSinkFailure> {
        self.try_send(RouteCommandKind::Publish { batch })
    }

    fn try_send(&mut self, kind: RouteCommandKind) -> Result<(), ProductionSinkFailure> {
        self.check_failure()?;
        let retained_bytes = kind
            .retained_bytes()
            .ok_or(ProductionSinkFailure::ActivationBufferBytesSaturated)?;
        let byte_charge = u32::try_from(retained_bytes)
            .map_err(|_error| ProductionSinkFailure::ActivationBufferBytesSaturated)?;
        let command_permit = Arc::clone(&self.command_budget)
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => {
                    ProductionSinkFailure::ActivationBufferCountSaturated
                }
                tokio::sync::TryAcquireError::Closed => {
                    ProductionSinkFailure::ActivationWorkerClosed
                }
            })?;
        let byte_permit = Arc::clone(&self.byte_budget)
            .try_acquire_many_owned(byte_charge)
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => {
                    ProductionSinkFailure::ActivationBufferBytesSaturated
                }
                tokio::sync::TryAcquireError::Closed => {
                    ProductionSinkFailure::ActivationWorkerClosed
                }
            })?;
        let command = RouteCommand {
            kind,
            _ticket: RouteBufferTicket {
                _command_permit: command_permit,
                _byte_permit: byte_permit,
            },
        };
        match self.commands.try_send(command) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_command)) => {
                Err(ProductionSinkFailure::ActivationBufferCountSaturated)
            }
            Err(mpsc::error::TrySendError::Closed(_command)) => {
                self.check_failure()?;
                Err(ProductionSinkFailure::ActivationWorkerClosed)
            }
        }
    }

    pub(super) fn check_failure(&mut self) -> Result<(), ProductionSinkFailure> {
        let worker_closed = self.status.has_changed().is_err();
        let failure = *self.status.borrow_and_update();
        if let Some(failure) = failure {
            Err(ProductionSinkFailure::RouteActivation(failure))
        } else if worker_closed {
            Err(ProductionSinkFailure::ActivationWorkerClosed)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
pub(super) enum RouteActivationBinding {
    Initial,
    Replacement(DormantRouteIngress),
}

#[derive(Debug)]
struct RouteBufferTicket {
    _command_permit: OwnedSemaphorePermit,
    _byte_permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
struct RouteCommand {
    kind: RouteCommandKind,
    _ticket: RouteBufferTicket,
}

#[derive(Debug)]
enum RouteCommandKind {
    Activate {
        dormant: Option<DormantRouteIngress>,
        batch: CurrentDecodedProviderBatch,
    },
    Publish {
        batch: CurrentDecodedProviderBatch,
    },
}

impl RouteCommandKind {
    fn retained_bytes(&self) -> Option<usize> {
        let command_and_batch = std::mem::size_of::<RouteCommand>().checked_add(match self {
            Self::Activate { batch, .. } | Self::Publish { batch } => batch.retained_bytes(),
        })?;
        match self {
            Self::Activate {
                dormant: Some(dormant),
                ..
            } => command_and_batch.checked_add(dormant.retained_bytes()),
            Self::Activate { dormant: None, .. } | Self::Publish { .. } => Some(command_and_batch),
        }
    }
}

#[derive(Debug)]
struct RouteActor {
    initial: Option<DormantRouteIngress>,
    active: Option<BoundShardIngress>,
}

pub(super) fn spawn_route_activation(
    dormant: DormantRouteIngress,
    limits: RouteBufferLimits,
    cancellation: CancellationToken,
) -> (RouteActivationPublisher, RouteActorWorker) {
    let route = dormant.route().clone();
    let command_budget = Arc::new(Semaphore::new(limits.command_count.get()));
    let byte_budget = Arc::new(Semaphore::new(limits.batch_bytes.get() as usize));
    let (commands, mut command_receiver) = mpsc::channel(limits.command_count.get());
    let (status_sender, status) = watch::channel(None);
    let worker = tokio::spawn(async move {
        let mut actor = RouteActor {
            initial: Some(dormant),
            active: None,
        };
        let run_result = loop {
            let command = tokio::select! {
                biased;
                () = cancellation.cancelled() => break Ok(()),
                command = command_receiver.recv() => match command {
                    Some(command) => command,
                    None => break Ok(()),
                }
            };
            match actor.process(command, cancellation.clone()).await {
                Ok(()) => {}
                Err(RouteActivationFailure::Bind(LiveIngressBindError::Cancelled))
                    if cancellation.is_cancelled() =>
                {
                    break Ok(());
                }
                Err(failure) => {
                    status_sender.send_replace(Some(failure));
                    break Err(failure);
                }
            }
        };
        let revoke_result = actor
            .revoke_active_generation()
            .await
            .map_err(RouteActivationFailure::Revoke);
        match (run_result, revoke_result) {
            (_, Err(revoke)) => Err(revoke),
            (Err(run), Ok(())) => Err(run),
            (Ok(()), Ok(())) => Ok(()),
        }
    });
    (
        RouteActivationPublisher {
            route,
            commands,
            command_budget,
            byte_budget,
            status,
            initial_activation_available: true,
        },
        worker,
    )
}

impl RouteActor {
    async fn process(
        &mut self,
        command: RouteCommand,
        cancellation: CancellationToken,
    ) -> Result<(), RouteActivationFailure> {
        let RouteCommand { kind, _ticket } = command;
        let outcome = match kind {
            RouteCommandKind::Activate { dormant, batch } => {
                let dormant = match dormant {
                    Some(dormant) => dormant,
                    None => self
                        .initial
                        .take()
                        .ok_or(RouteActivationFailure::CommandOrder)?,
                };
                let ingress = dormant
                    .activate(batch.current_lease().clone(), cancellation)
                    .await
                    .map_err(RouteActivationFailure::Bind)?;
                if let Err(error) = ingress.try_publish(batch) {
                    ingress
                        .revoke_generation()
                        .await
                        .map_err(RouteActivationFailure::Revoke)?;
                    return Err(RouteActivationFailure::Ingress(error));
                }
                self.active = Some(ingress);
                Ok(())
            }
            RouteCommandKind::Publish { batch } => self
                .active
                .as_ref()
                .ok_or(RouteActivationFailure::CommandOrder)?
                .try_publish(batch)
                .map_err(RouteActivationFailure::Ingress),
        };
        drop(_ticket);
        outcome
    }

    async fn revoke_active_generation(&mut self) -> Result<(), LiveIngressRevokeError> {
        match self.active.take() {
            Some(active) => active.revoke_generation().await,
            None => Ok(()),
        }
    }
}
