//! Retained bounded raw-store reconciliation on the existing supervised I/O owner.
use super::*;
use market_squawk_platform::{
    SealedResearchRecoveryAdmission, SealedResearchRecoverySession, SealedResearchRecoveryTurn,
};

/// Installed-store cursor. It owns no writer transaction and no inventory-sized collection.
#[derive(Debug)]
pub struct ProviderCaptureRecovery {
    store: Arc<market_squawk_platform::SealedResearchJournalStore>,
    session: SealedResearchRecoverySession,
}
impl AnalyticalDataService {
    /// Opens the directory cursor only; startup never waits for historical hashing.
    pub fn create_provider_capture_recovery(
        &self,
        store: Arc<market_squawk_platform::SealedResearchJournalStore>,
    ) -> Result<ProviderCaptureRecovery, IngestError> {
        let session = store
            .begin_recovery()
            .map_err(map_provider_recovery_store_error)?;
        Ok(ProviderCaptureRecovery { store, session })
    }

    /// Performs one finite turn and retains partial hashing for the next owned turn.
    pub async fn recover_provider_capture_store_turn(
        &self,
        recovery: Arc<Mutex<ProviderCaptureRecovery>>,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<SealedResearchRecoveryTurn, IngestError> {
        check_market_event_read(deadline, cancellation)?;
        let operation = cancellation.child_token();
        let _cancel_on_drop = operation.clone().drop_guard();
        let supervisor = BlockingIoSupervisor::new(operation.clone());
        let manifests = Arc::clone(&self.manifests);
        let limits = self.catalog_read_limits;
        let token = operation.clone();
        let worker = supervisor
            .spawn_blocking(move || {
                let mut recovery = recovery
                    .try_lock()
                    .map_err(|_| IngestError::ProviderCaptureRecoveryWorkerUnavailable)?;
                let ProviderCaptureRecovery { store, session } = &mut *recovery;
                let control = MarketEventReadControl {
                    deadline,
                    cancellation: &token,
                };
                let mut catalog_failure = None;
                let result = session.advance(
                    store,
                    SealedResearchRecoveryAdmission::default(),
                    &control,
                    |kind, digest| {
                        // This read begins AFTER the platform pin check while publication/reopen is
                        // excluded. A snapshot retained from before that check could miss a new commit.
                        let result = manifests.read_snapshot(limits, deadline, &token).and_then(
                            |snapshot| {
                                snapshot.read(|snapshot| {
                                    snapshot.authoritative_provider_raw_claim(kind, digest)
                                })
                            },
                        );
                        match result {
                            Ok(claim) => Ok(claim),
                            Err(error) => {
                                catalog_failure = Some(error);
                                Err(ResearchObjectControlError::Unavailable)
                            }
                        }
                    },
                );
                if let Some(error) = catalog_failure {
                    return Err(map_market_recovery_catalog_error(error));
                }
                result.map_err(map_provider_recovery_store_error)
            })
            .map_err(map_provider_recovery_admission_error)?;
        let outcome = worker
            .await
            .map_err(|_| IngestError::ProviderCaptureRecoveryWorkerUnavailable)?;
        supervisor.cancel();
        supervisor.wait_idle().await;
        outcome
    }
}
