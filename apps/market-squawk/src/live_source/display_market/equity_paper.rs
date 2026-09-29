//! Opt-in exact-generation simulation reads using the existing actor read budgets.
use super::*;
impl DisplayMarketDirectory {
    pub(crate) async fn virtual_paper_snapshot(
        &self,
        key: &DisplayMarketKey,
        at: Timestamp,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<DisplayMarketSnapshotLease, DisplayMarketReadError> {
        require_read_time(cancellation, deadline)?;
        let entries = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(DisplayMarketReadError::Cancelled),
            () = self.inner.cancellation.cancelled() => return Err(DisplayMarketReadError::WorkerClosed),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => return Err(DisplayMarketReadError::Deadline),
            entries = self.inner.entries.lock() => entries,
        };
        let entry = entries
            .iter()
            .find(|entry| entry.key.as_ref() == key)
            .ok_or(DisplayMarketReadError::Unavailable)?;
        if entry.unregistering || !entry.read_admission.is_admitted() {
            return Err(DisplayMarketReadError::Unregistering);
        }
        let client = entry.read_client.clone();
        drop(entries);
        let snapshot = client.snapshot(at, cancellation, deadline, true).await?;
        require_read_time(cancellation, deadline)?;
        Ok(snapshot)
    }
}
