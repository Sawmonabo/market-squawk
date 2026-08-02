//! Object-safe asynchronous authority over the composition-owned catalog writer.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use futures_util::future::BoxFuture;
use market_squawk_sources::{
    ObservedRevisionAssignments, ObservedRevisionAuthority, ObservedRevisionBatch,
    ObservedRevisionError,
};
use tokio_util::sync::CancellationToken;

use super::check_operation;
use crate::CatalogAuthority;
use crate::blocking_supervisor::{BlockingIoAdmissionError, BlockingIoSupervisor};

/// Cloneable least-authority view over the one composition-owned catalog writer.
#[derive(Clone)]
pub(crate) struct CatalogObservedRevisionAuthority {
    authority: Arc<Mutex<CatalogAuthority>>,
}

impl CatalogObservedRevisionAuthority {
    pub(crate) fn new(authority: Arc<Mutex<CatalogAuthority>>) -> Self {
        Self { authority }
    }
}

impl fmt::Debug for CatalogObservedRevisionAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CatalogObservedRevisionAuthority([SHARED CATALOG AUTHORITY])")
    }
}

impl ObservedRevisionAuthority for CatalogObservedRevisionAuthority {
    fn assign(
        &self,
        batch: ObservedRevisionBatch,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ObservedRevisionAssignments, ObservedRevisionError>> {
        let authority = Arc::clone(&self.authority);
        Box::pin(async move {
            check_operation(deadline, &cancellation)?;
            let supervisor = BlockingIoSupervisor::new(cancellation.clone());
            let worker_cancellation = cancellation.clone();
            let task = supervisor
                .spawn_blocking(move || {
                    check_operation(deadline, &worker_cancellation)?;
                    let authority = authority
                        .lock()
                        .map_err(|_| ObservedRevisionError::PersistenceUnavailable)?;
                    check_operation(deadline, &worker_cancellation)?;
                    authority.catalog().assign_observed_revisions(
                        batch,
                        deadline,
                        &worker_cancellation,
                    )
                })
                .map_err(map_admission_error)?;
            task.await
                .map_err(|_| ObservedRevisionError::PersistenceUnavailable)?
        })
    }
}

fn map_admission_error(error: BlockingIoAdmissionError) -> ObservedRevisionError {
    match error {
        BlockingIoAdmissionError::Cancelled => ObservedRevisionError::Cancelled,
        BlockingIoAdmissionError::Saturated | BlockingIoAdmissionError::ReaperUnavailable => {
            ObservedRevisionError::PersistenceUnavailable
        }
    }
}
