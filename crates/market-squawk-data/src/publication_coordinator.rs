//! Bounded async exclusion for final-object publication and orphan recovery.

use std::fmt;
use std::sync::Arc;

use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Default)]
pub(crate) struct PublicationCoordinator {
    writer: Arc<Mutex<()>>,
}

impl PublicationCoordinator {
    pub(crate) async fn acquire(
        &self,
        cancellation: &CancellationToken,
    ) -> Option<PublicationLease> {
        tokio::select! {
            lease = Arc::clone(&self.writer).lock_owned() => Some(PublicationLease {
                owner: Arc::clone(&self.writer),
                _lease: lease,
            }),
            _ = cancellation.cancelled() => None,
        }
    }

    pub(crate) async fn acquire_recovery(&self) -> PublicationLease {
        PublicationLease {
            owner: Arc::clone(&self.writer),
            _lease: Arc::clone(&self.writer).lock_owned().await,
        }
    }

    pub(crate) fn owns(&self, lease: &PublicationLease) -> bool {
        Arc::ptr_eq(&self.writer, &lease.owner)
    }
}

/// Non-cloneable authority proving exclusive ownership of final-object publication/recovery.
pub struct PublicationLease {
    owner: Arc<Mutex<()>>,
    _lease: OwnedMutexGuard<()>,
}

impl fmt::Debug for PublicationLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PublicationLease")
            .field("owner", &"[PUBLICATION COORDINATOR]")
            .finish_non_exhaustive()
    }
}
