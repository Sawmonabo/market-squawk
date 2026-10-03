//! Exact automatic-receipt reads while the existing backup lease fences the sole writer.

use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};

use market_squawk_services::{RequestContext, ServiceError};
use market_squawk_valuation::{
    AutomaticValuationMethodReceipt, EvidenceOrigin, FairValueService, MeasurementId,
};
use tokio::sync::OwnedMutexGuard;
use tokio_util::sync::CancellationToken;

use super::{FairValueBackupAttestation, FairValueBackupError, FairValueDomainService};
use crate::application::domain_support::ensure_request_live;

/// Private owner-issued view of the already attested state. Sharing only immutable access keeps
/// the same writer fence alive; it neither copies the catalog nor constructs valuation evidence.
pub(super) struct BackupAutomaticValuationRead {
    state: OwnedMutexGuard<FairValueService>,
    attestation: FairValueBackupAttestation,
    cancellation: CancellationToken,
    active: AtomicBool,
}

/// Non-cloneable owner of the backup lifetime. Ordinary receipt reads may borrow its immutable
/// state without waiting for the writer mutex that this same lease deliberately retains.
pub(crate) struct FairValueBackupAttestationLease {
    read: Arc<BackupAutomaticValuationRead>,
    registration: Arc<Mutex<Weak<BackupAutomaticValuationRead>>>,
}

impl FairValueBackupAttestationLease {
    pub(crate) fn attestation(&self) -> FairValueBackupAttestation {
        self.read.attestation
    }

    /// Recompute the complete attestation under the unchanged original mutation fence.
    pub(crate) fn revalidate(&self) -> Result<(), FairValueBackupError> {
        if self.read.cancellation.is_cancelled() {
            return Err(FairValueBackupError::Cancelled);
        }
        if !self.read.active.load(Ordering::Acquire)
            || FairValueBackupAttestation::try_from_service(&self.read.state)?
                != self.read.attestation
        {
            return Err(FairValueBackupError::CatalogMismatch);
        }
        Ok(())
    }
}

impl Drop for FairValueBackupAttestationLease {
    fn drop(&mut self) {
        // Revoke new view reads before unregistering. An in-flight read retains the writer guard
        // until its final validity check and local Arc drop; no detached serving handle escapes.
        self.read.active.store(false, Ordering::Release);
        let mut registration = match self.registration.lock() {
            Ok(registration) => registration,
            Err(poisoned) => poisoned.into_inner(),
        };
        if registration.ptr_eq(&Arc::downgrade(&self.read)) {
            *registration = Weak::new();
        }
    }
}

impl std::fmt::Debug for FairValueBackupAttestationLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("FairValueBackupAttestationLease([RETAINED FAIR-VALUE WRITER])")
    }
}

impl FairValueDomainService {
    /// Retains the sole writer and publishes an immutable receipt view only after attestation.
    pub(crate) async fn retain_backup_attestation(
        self: &Arc<Self>,
        cancellation: &CancellationToken,
    ) -> Result<FairValueBackupAttestationLease, FairValueBackupError> {
        let state = Arc::clone(&self.state);
        let guard = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(FairValueBackupError::Cancelled),
            guard = state.lock_owned() => guard,
        };
        if cancellation.is_cancelled() {
            return Err(FairValueBackupError::Cancelled);
        }
        let attestation = FairValueBackupAttestation::try_from_service(&guard)?;
        let read = Arc::new(BackupAutomaticValuationRead {
            state: guard,
            attestation,
            cancellation: cancellation.clone(),
            active: AtomicBool::new(true),
        });
        {
            let mut registration = self
                .backup_automatic_read
                .lock()
                .map_err(|_| FairValueBackupError::CatalogMismatch)?;
            if cancellation.is_cancelled() {
                return Err(FairValueBackupError::Cancelled);
            }
            // The sole writer guard rules out a different live lease. Never replace one if the
            // service's private registration nevertheless disagrees with that invariant.
            if registration.upgrade().is_some() {
                return Err(FairValueBackupError::CatalogMismatch);
            }
            *registration = Arc::downgrade(&read);
        }
        Ok(FairValueBackupAttestationLease {
            read,
            registration: Arc::clone(&self.backup_automatic_read),
        })
    }

    /// Reads one original receipt from canonical retained state. This does not authorize its
    /// current use; existing callers must still reopen sources, rights and financial validity.
    pub(crate) async fn read_automatic_valuation(
        &self,
        measurement_id: MeasurementId,
        context: &RequestContext,
    ) -> Result<AutomaticValuationMethodReceipt, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        let retained = {
            self.backup_automatic_read
                .lock()
                .map_err(|_| ServiceError::Unavailable)?
                .upgrade()
        };
        if let Some(retained) = retained {
            check_retained(&retained)?;
            let receipt = automatic_receipt(&retained.state, measurement_id)?;
            ensure_request_live(context, &self.lifecycle)?;
            check_retained(&retained)?;
            return Ok(receipt);
        }
        let state = self.lock_state(context).await?;
        let receipt = automatic_receipt(&state, measurement_id)?;
        drop(state);
        ensure_request_live(context, &self.lifecycle)?;
        Ok(receipt)
    }
}

fn check_retained(retained: &BackupAutomaticValuationRead) -> Result<(), ServiceError> {
    if !retained.active.load(Ordering::Acquire) || retained.cancellation.is_cancelled() {
        // Cancellation of a backup cannot cancel a different caller's request. Its view is
        // simply unavailable; that caller's own cancellation/deadline is checked separately.
        return Err(ServiceError::Unavailable);
    }
    Ok(())
}

pub(super) fn automatic_receipt(
    state: &FairValueService,
    measurement_id: MeasurementId,
) -> Result<AutomaticValuationMethodReceipt, ServiceError> {
    let measurement = state
        .measurement(measurement_id)
        .ok_or(ServiceError::NotFound)?;
    let [input] = measurement.inputs() else {
        return Err(ServiceError::InvalidResult);
    };
    let EvidenceOrigin::AutomaticValuation { receipt } = input.evidence().origin() else {
        return Err(ServiceError::NotFound);
    };
    Ok(receipt.as_ref().clone())
}
