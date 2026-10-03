//! Owner-issued access to exact automatic receipts without valuation-production authority.

use std::{fmt, sync::Arc, time::Instant};

use market_squawk_services::{RequestContext, ServiceError};
use market_squawk_valuation::{AutomaticValuationMethodReceipt, FairValueService, MeasurementId};

use super::{FairValueBackupAttestation, FairValueBackupError, FairValueDomainService};

/// Reads original automatic receipts from an installed owner or an attested restored owner.
/// The private variants expose no state mutation, producer selection, or synthetic receipts.
#[derive(Clone)]
pub(crate) struct FairValueAutomaticReadCapability {
    owner: AutomaticReadOwner,
}

#[derive(Clone)]
enum AutomaticReadOwner {
    Installed(Arc<FairValueDomainService>),
    Restored(Arc<RestoredAutomaticRead>),
}

struct RestoredAutomaticRead {
    service: FairValueService,
    attestation: FairValueBackupAttestation,
}

impl FairValueDomainService {
    /// Shares this exact installed owner, including its lifecycle and immutable backup view.
    pub(crate) fn automatic_read_capability(self: &Arc<Self>) -> FairValueAutomaticReadCapability {
        FairValueAutomaticReadCapability {
            owner: AutomaticReadOwner::Installed(Arc::clone(self)),
        }
    }
}

impl FairValueAutomaticReadCapability {
    /// Consumes the actual service reopened and verified against the restore manifest's
    /// attestation. Recomputing its catalog identity checks the backing head at handoff. The capability
    /// retains immutable ownership; it does not create producer selectors or another writer.
    pub(crate) fn from_restored_service(
        service: FairValueService,
    ) -> Result<Self, FairValueBackupError> {
        let attestation = FairValueBackupAttestation::try_from_service(&service)?;
        Ok(Self {
            owner: AutomaticReadOwner::Restored(Arc::new(RestoredAutomaticRead {
                service,
                attestation,
            })),
        })
    }

    /// Returns the unchanged original sealed receipt. Source reopening, current rights, and
    /// original financial admission remain the caller's separate A/B/C reconstruction duties.
    pub(crate) async fn read_automatic_valuation(
        &self,
        measurement_id: MeasurementId,
        context: &RequestContext,
    ) -> Result<AutomaticValuationMethodReceipt, ServiceError> {
        match &self.owner {
            AutomaticReadOwner::Installed(owner) => {
                owner
                    .read_automatic_valuation(measurement_id, context)
                    .await
            }
            AutomaticReadOwner::Restored(owner) => {
                ensure_live(context)?;
                if FairValueBackupAttestation::try_from_service(&owner.service)
                    .map_err(|_| ServiceError::InvalidResult)?
                    != owner.attestation
                {
                    return Err(ServiceError::InvalidResult);
                }
                let receipt =
                    super::backup_read::automatic_receipt(&owner.service, measurement_id)?;
                ensure_live(context)?;
                Ok(receipt)
            }
        }
    }
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

impl fmt::Debug for FairValueAutomaticReadCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FairValueAutomaticReadCapability([OWNER-ISSUED RECEIPT READ])")
    }
}
