//! Account-wide ordering for Direct catalog publication and live generation replacement.
//!
//! Product owners retain their registries, physical reference seals, and cleanup. This barrier
//! supplies no identity or health authority: it only prevents one product's catalog mutation
//! from revoking a peer that is already using the account's next live generation.

use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{Barrier, Mutex, MutexGuard, watch};
use tokio_util::sync::CancellationToken;

use crate::provider_activation::COINBASE_DIRECT_MAXIMUM_SUBSCRIPTIONS;

/// Shared by precisely the product owners admitted at account startup.
#[derive(Clone, Debug)]
pub(super) struct DirectAccountCoordinator {
    state: Arc<Mutex<AccountState>>,
    published: watch::Sender<Option<Arc<EpochState>>>,
    cancellation: CancellationToken,
}

#[derive(Debug)]
struct AccountState {
    slots: [bool; COINBASE_DIRECT_MAXIMUM_SUBSCRIPTIONS],
    arrived: [bool; COINBASE_DIRECT_MAXIMUM_SUBSCRIPTIONS],
    count: usize,
    arrivals: usize,
    current: Option<Arc<EpochState>>,
}

#[derive(Debug)]
struct EpochState {
    id: u64,
    cancellation: CancellationToken,
    catalog_publication: Mutex<()>,
    catalog_synchronized: Barrier,
    selected: Barrier,
}

impl DirectAccountCoordinator {
    pub(super) fn try_new(
        slots: &[usize],
        cancellation: CancellationToken,
    ) -> Result<Self, DirectAccountCoordinatorError> {
        if slots.is_empty() || slots.len() > COINBASE_DIRECT_MAXIMUM_SUBSCRIPTIONS {
            return Err(DirectAccountCoordinatorError::Topology);
        }
        let mut admitted = [false; COINBASE_DIRECT_MAXIMUM_SUBSCRIPTIONS];
        for slot in slots {
            let admitted = admitted
                .get_mut(*slot)
                .ok_or(DirectAccountCoordinatorError::Topology)?;
            if *admitted {
                return Err(DirectAccountCoordinatorError::Topology);
            }
            *admitted = true;
        }
        let (published, _receiver) = watch::channel(None);
        Ok(Self {
            state: Arc::new(Mutex::new(AccountState {
                slots: admitted,
                arrived: [false; COINBASE_DIRECT_MAXIMUM_SUBSCRIPTIONS],
                count: slots.len(),
                arrivals: 0,
                current: None,
            })),
            published,
            cancellation,
        })
    }

    /// Joins only after this product has reaped every owner of its previous generation.
    ///
    /// The first returning product cancels the old epoch. No new preflight or catalog mutation
    /// may begin until every admitted product has joined, including peers cancelled for restart.
    pub(super) async fn join_next_epoch(
        &self,
        slot: usize,
        previous_epoch: Option<u64>,
    ) -> Result<DirectAccountEpoch, DirectAccountCoordinatorError> {
        let mut published = self.published.subscribe();
        {
            let mut state = tokio::select! {
                biased;
                () = self.cancellation.cancelled() => {
                    return Err(DirectAccountCoordinatorError::Cancelled);
                }
                state = self.state.lock() => state,
            };
            if !state.slots.get(slot).copied().unwrap_or(false) {
                return Err(DirectAccountCoordinatorError::Topology);
            }
            if state.current.as_ref().map(|epoch| epoch.id) != previous_epoch || state.arrived[slot]
            {
                return Err(DirectAccountCoordinatorError::Sequence);
            }
            if let Some(epoch) = &state.current {
                epoch.cancellation.cancel();
            }
            state.arrived[slot] = true;
            state.arrivals += 1;
            if state.arrivals == state.count {
                let id = previous_epoch
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or(DirectAccountCoordinatorError::EpochExhausted)?;
                let epoch = Arc::new(EpochState {
                    id,
                    cancellation: self.cancellation.child_token(),
                    catalog_publication: Mutex::new(()),
                    catalog_synchronized: Barrier::new(state.count),
                    selected: Barrier::new(state.count),
                });
                state.arrivals = 0;
                state.arrived.fill(false);
                state.current = Some(Arc::clone(&epoch));
                let _previous = self.published.send_replace(Some(epoch));
            }
        }
        loop {
            if self.cancellation.is_cancelled() {
                return Err(DirectAccountCoordinatorError::Cancelled);
            }
            let current = published.borrow_and_update().clone();
            if let Some(epoch) = current
                && Some(epoch.id) != previous_epoch
            {
                return Ok(DirectAccountEpoch {
                    epoch,
                    stage: EpochStage::Synchronizing,
                });
            }
            tokio::select! {
                biased;
                () = self.cancellation.cancelled() => {
                    return Err(DirectAccountCoordinatorError::Cancelled);
                }
                changed = published.changed() => {
                    changed.map_err(|_| DirectAccountCoordinatorError::Sequence)?;
                }
            }
        }
    }

    /// A startup notification is usable only while every product still owns this same epoch.
    pub(super) async fn is_current_epoch(&self, id: u64) -> bool {
        if self.cancellation.is_cancelled() {
            return false;
        }
        self.state
            .lock()
            .await
            .current
            .as_ref()
            .is_some_and(|epoch| epoch.id == id && !epoch.cancellation.is_cancelled())
    }
}

#[derive(Debug, PartialEq, Eq)]
enum EpochStage {
    Synchronizing,
    Selecting,
    Live,
}

/// One product's non-cloneable participation in an account epoch.
#[derive(Debug)]
pub(super) struct DirectAccountEpoch {
    epoch: Arc<EpochState>,
    stage: EpochStage,
}

impl DirectAccountEpoch {
    pub(super) fn id(&self) -> u64 {
        self.epoch.id
    }

    /// Account cancellation reaches this token; restarting the epoch does not cancel the account.
    pub(super) fn cancellation(&self) -> CancellationToken {
        self.epoch.cancellation.clone()
    }

    /// Serializes each read/CAS publication within this epoch so peers that share a canonical
    /// instrument retain one another's accepted identities.
    pub(super) async fn catalog_publication(
        &self,
    ) -> Result<MutexGuard<'_, ()>, DirectAccountCoordinatorError> {
        let guard = tokio::select! {
            biased;
            () = self.epoch.cancellation.cancelled() => {
                return Err(DirectAccountCoordinatorError::Cancelled);
            }
            guard = self.epoch.catalog_publication.lock() => guard,
        };
        if self.epoch.cancellation.is_cancelled() {
            return Err(DirectAccountCoordinatorError::Cancelled);
        }
        Ok(guard)
    }

    /// Call after physical reference sealing, validated decode, and successful catalog CAS.
    /// No product may select an identity until this barrier completes for every product.
    pub(super) async fn catalog_synchronized(
        &mut self,
    ) -> Result<(), DirectAccountCoordinatorError> {
        if self.stage != EpochStage::Synchronizing {
            self.request_restart();
            return Err(DirectAccountCoordinatorError::Sequence);
        }
        self.wait(&self.epoch.catalog_synchronized).await?;
        self.stage = EpochStage::Selecting;
        Ok(())
    }

    /// Call after this product's registry successfully selected its synchronized identity.
    /// A source session, route actor, or live connection may start only after this returns.
    pub(super) async fn selected(&mut self) -> Result<(), DirectAccountCoordinatorError> {
        if self.stage != EpochStage::Selecting {
            self.request_restart();
            return Err(DirectAccountCoordinatorError::Sequence);
        }
        self.wait(&self.epoch.selected).await?;
        self.stage = EpochStage::Live;
        Ok(())
    }

    pub(super) fn request_restart(&self) {
        self.epoch.cancellation.cancel();
    }

    async fn wait(&self, barrier: &Barrier) -> Result<(), DirectAccountCoordinatorError> {
        tokio::select! {
            biased;
            () = self.epoch.cancellation.cancelled() => {
                return Err(DirectAccountCoordinatorError::Cancelled);
            }
            _arrival = barrier.wait() => {}
        }
        // A peer may fail at the instant the last arrival releases the barrier.
        if self.epoch.cancellation.is_cancelled() {
            return Err(DirectAccountCoordinatorError::Cancelled);
        }
        Ok(())
    }
}

impl Drop for DirectAccountEpoch {
    fn drop(&mut self) {
        self.request_restart();
    }
}

/// Fail-closed account lifecycle errors; none imply provider health or identity acceptance.
#[derive(Debug, Error)]
pub enum DirectAccountCoordinatorError {
    /// The account's exact bounded product membership is invalid.
    #[error("Direct account product topology is invalid")]
    Topology,
    /// A product repeated or skipped an account epoch or its required phase.
    #[error("Direct account generation ordering is invalid")]
    Sequence,
    /// A new account epoch cannot be represented without wrapping.
    #[error("Direct account generation sequence is exhausted")]
    EpochExhausted,
    /// The account stopped or one of its products requested a coordinated restart.
    #[error("Direct account generation was cancelled")]
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::DirectAccountCoordinator;
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn both_products_finish_each_phase_and_rejoin_after_restart() {
        let account = CancellationToken::new();
        let coordinator = DirectAccountCoordinator::try_new(&[0, 1], account.clone())
            .expect("two distinct slots are admitted");
        let (first, second) = tokio::join!(
            coordinator.join_next_epoch(0, None),
            coordinator.join_next_epoch(1, None),
        );
        let mut first = first.expect("first product joins");
        let mut second = second.expect("second product joins");
        let old_epoch = second.cancellation();

        let (arriving, arrived) = oneshot::channel();
        let first_phase = tokio::spawn(async move {
            arriving.send(()).expect("phase observer remains open");
            first.catalog_synchronized().await.expect("catalog barrier");
            first.selected().await.expect("selection barrier");
            first
        });
        arrived
            .await
            .expect("first product reaches catalog barrier");
        tokio::task::yield_now().await;
        assert!(
            !first_phase.is_finished(),
            "one product cannot pass catalog alone"
        );
        second
            .catalog_synchronized()
            .await
            .expect("catalog barrier");
        tokio::task::yield_now().await;
        assert!(
            !first_phase.is_finished(),
            "one product cannot pass selection alone"
        );
        second.selected().await.expect("selection barrier");
        let first = first_phase.await.expect("first product task completes");

        first.request_restart();
        assert!(old_epoch.is_cancelled(), "restart cancels the peer");
        drop(first);
        drop(second);
        let (next_first, next_second) = tokio::join!(
            coordinator.join_next_epoch(0, Some(1)),
            coordinator.join_next_epoch(1, Some(1)),
        );
        assert_eq!(next_first.expect("first rejoins").id(), 2);
        assert_eq!(next_second.expect("second rejoins").id(), 2);
        account.cancel();
    }
}
