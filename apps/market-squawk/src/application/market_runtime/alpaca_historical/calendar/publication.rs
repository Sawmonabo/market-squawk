//! Calendar publication under the existing account runtime's operation drain.

use std::{fmt, sync::Arc, time::Instant};

use market_squawk_data::{CatalogAuthority, IngestError, IngestPrecommitAuthority};
use tokio_util::sync::CancellationToken;

use crate::provider_activation::ProviderAccountPublicationAuthority;

use super::super::{
    AlpacaHistoricalCapabilityError, AlpacaHistoricalOperation, AlpacaHistoricalRuntimeCapability,
    ensure_before,
};

impl AlpacaHistoricalRuntimeCapability {
    /// Retains one already-admitted account operation through the immutable calendar commit.
    ///
    /// The producer acquires this after authenticated fetching and normalization, before the
    /// existing research ingest. It retains the account's existing activation mutation guard
    /// until ingest completes, then drops this authority before ordinary runtime checks.
    /// Shutdown cancels its authority and drains the same historical operation counter.
    pub(crate) async fn acquire_calendar_publication_authority(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Arc<dyn IngestPrecommitAuthority>, AlpacaHistoricalCapabilityError> {
        ensure_before(deadline, cancellation)?;
        let operation = self.inner.admit()?;
        self.require_current(deadline, cancellation).await?;
        ensure_before(deadline, cancellation)?;
        let account = tokio::select! {
            biased;
            () = self.inner.cancellation.cancelled() => {
                return Err(AlpacaHistoricalCapabilityError::Revoked);
            }
            () = cancellation.cancelled() => {
                return Err(AlpacaHistoricalCapabilityError::Cancelled);
            }
            () = tokio::time::sleep_until(deadline.into()) => {
                return Err(AlpacaHistoricalCapabilityError::DeadlineExceeded);
            }
            result = self.inner.account_currentness.acquire_publication_authority() => {
                result.map_err(|_| AlpacaHistoricalCapabilityError::Stale)?
            }
        };
        self.inner.ensure_usable()?;
        ensure_before(deadline, cancellation)?;
        Ok(Arc::new(CalendarPublicationAuthority {
            runtime: self.clone(),
            account,
            _operation: operation,
            deadline,
            cancellation: cancellation.clone(),
        }))
    }
}

struct CalendarPublicationAuthority {
    runtime: AlpacaHistoricalRuntimeCapability,
    account: ProviderAccountPublicationAuthority,
    _operation: AlpacaHistoricalOperation,
    deadline: Instant,
    cancellation: CancellationToken,
}

impl fmt::Debug for CalendarPublicationAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CalendarPublicationAuthority")
            .field("generation", &self.runtime.group_generation())
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

impl IngestPrecommitAuthority for CalendarPublicationAuthority {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        self.validate_operation()?;
        self.account
            .require_current()
            .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        self.validate_operation()
    }

    fn validate_catalog_precommit(&self, catalog: &CatalogAuthority) -> Result<(), IngestError> {
        self.validate_operation()?;
        self.account
            .require_catalog_current(catalog)
            .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        self.validate_operation()
    }
}

impl CalendarPublicationAuthority {
    fn validate_operation(&self) -> Result<(), IngestError> {
        ensure_before(self.deadline, &self.cancellation).map_err(map_capability_error)?;
        self.runtime
            .inner
            .ensure_usable()
            .map_err(map_capability_error)?;
        ensure_before(self.deadline, &self.cancellation).map_err(map_capability_error)
    }
}

const fn map_capability_error(error: AlpacaHistoricalCapabilityError) -> IngestError {
    match error {
        AlpacaHistoricalCapabilityError::Cancelled => IngestError::Cancelled,
        AlpacaHistoricalCapabilityError::DeadlineExceeded => IngestError::DeadlineExceeded,
        AlpacaHistoricalCapabilityError::Revoked | AlpacaHistoricalCapabilityError::Stale => {
            IngestError::PublicationAuthorityRevoked
        }
    }
}
