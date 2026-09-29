//! Explicit civil-date context demand through the existing sole account group.

use super::*;
use crate::application::market_calendar::context::{
    MarketSessionContextRequest, MarketSessionProduct,
};
use market_squawk_adapter_schwab::MarketId;
use market_squawk_data::CommittedDataset;

impl MarketRuntimeRegistry {
    pub(crate) async fn verify_market_session_context_reference(
        &self,
        request: &MarketSessionContextRequest,
        manifest: &market_squawk_data::DatasetManifestRef,
        binding_digest: EvidenceDigest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), ServiceError> {
        check_context_request(self, deadline, &cancellation)?;
        self.provider_activation
            .verify_market_session_context_reference(
                native_market(request.product()),
                request.date(),
                manifest,
                binding_digest,
                deadline,
                &cancellation,
            )
            .await
    }

    /// Returns one original source publication and its exact physical binding. The neutral reader
    /// retains the same request and reopens this manifest; returned entries grant no completeness.
    pub(crate) async fn acquire_market_session_context(
        &self,
        request: &MarketSessionContextRequest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(CommittedDataset, EvidenceDigest), ServiceError> {
        check_context_request(self, deadline, &cancellation)?;
        let surface = try_surface_identifier(AccountMarketSurface::SchwabMarketData)?;
        let owner = {
            let entries = bounded_lock(&self.entries, deadline, &cancellation).await?;
            let entry = entries
                .iter()
                .find(|entry| entry.surface_id == surface)
                .ok_or(ServiceError::Unavailable)?;
            let MarketRuntime::Account(group) = &entry.runtime else {
                return Err(ServiceError::Unavailable);
            };
            group
                .schwab_account_owner()
                .ok_or(ServiceError::Unavailable)?
        };
        let market = native_market(request.product());
        // Do not hold the registry mutation/entry lock across provider I/O. Original account and
        // OAuth precommit guards reject a concurrently retired owner, and the post-read check
        // below prevents returning a replacement group's result as current.
        let published = self
            .provider_activation
            .publish_schwab_market_hours(
                &owner,
                vec![market],
                request.date(),
                deadline,
                cancellation.child_token(),
            )
            .await?;
        check_context_request(self, deadline, &cancellation)?;
        let entries = bounded_lock(&self.entries, deadline, &cancellation).await?;
        let entry = entries
            .iter()
            .find(|entry| entry.surface_id == surface)
            .ok_or(ServiceError::Unavailable)?;
        let MarketRuntime::Account(group) = &entry.runtime else {
            return Err(ServiceError::Unavailable);
        };
        let current = group
            .schwab_account_owner()
            .ok_or(ServiceError::Unavailable)?;
        if !Arc::ptr_eq(&owner, &current) || !owner.currentness().is_active_now() {
            return Err(ServiceError::Unavailable);
        }
        Ok(published)
    }
}

fn native_market(product: MarketSessionProduct) -> MarketId {
    match product {
        MarketSessionProduct::Equity => MarketId::Equity,
        MarketSessionProduct::Option => MarketId::Option,
        MarketSessionProduct::Bond => MarketId::Bond,
        MarketSessionProduct::Future => MarketId::Future,
        MarketSessionProduct::Forex => MarketId::Forex,
    }
}

fn check_context_request(
    registry: &MarketRuntimeRegistry,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ServiceError::DeadlineExceeded)
    } else if registry.lifecycle.is_cancelled()
        || !registry
            .accepting
            .load(std::sync::atomic::Ordering::Acquire)
    {
        Err(ServiceError::Unavailable)
    } else {
        Ok(())
    }
}
