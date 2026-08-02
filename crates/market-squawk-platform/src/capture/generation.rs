//! Accounted capture-generation identity and fallible preparation.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use market_squawk_domain::{
    CaptureAuthorityBundle, CaptureAuthorityIdentity, CaptureDegradation, CaptureResidentToken,
    CaptureRetainedComponent, CaptureRetainedSizeError, checked_arc_value_allocation_bytes,
};
use thiserror::Error;

use super::accounting::{
    AccountingComponent, CaptureAccountingError, CaptureMemoryAccounting, CaptureMemoryReservation,
};

#[derive(Clone, Debug)]
pub(super) struct CaptureIdentitySnapshot(pub(super) Arc<AccountedGenerationIdentity>);

impl CaptureIdentitySnapshot {
    pub(super) fn identity(&self) -> &CaptureAuthorityIdentity {
        let _complete_retained_bytes = self.0.complete_retained_bytes;
        let _resident_reservation = &self.0.resident;
        &self.0.identity
    }
}

impl AsRef<CaptureAuthorityIdentity> for CaptureIdentitySnapshot {
    fn as_ref(&self) -> &CaptureAuthorityIdentity {
        self.identity()
    }
}

impl std::ops::Deref for CaptureIdentitySnapshot {
    type Target = CaptureAuthorityIdentity;

    fn deref(&self) -> &Self::Target {
        self.identity()
    }
}

impl PartialEq for CaptureIdentitySnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}

impl Eq for CaptureIdentitySnapshot {}

#[derive(Debug)]
pub(super) struct AccountedGenerationIdentity {
    pub(super) identity: CaptureAuthorityIdentity,
    complete_retained_bytes: usize,
    resident: CaptureMemoryReservation,
}

impl CaptureResidentToken for AccountedGenerationIdentity {}

#[derive(Debug)]
pub(super) struct GenerationCaptureState<B: CaptureAuthorityBundle> {
    pub(super) identity: Arc<AccountedGenerationIdentity>,
    pub(super) admission: std::sync::Mutex<B::Admission>,
    pub(super) degradation: B::Degradation,
    pub(super) accepting: AtomicBool,
}

impl<B: CaptureAuthorityBundle> GenerationCaptureState<B> {
    pub(super) fn new(
        identity: Arc<AccountedGenerationIdentity>,
        admission: B::Admission,
        degradation: B::Degradation,
    ) -> Self {
        Self {
            identity,
            admission: std::sync::Mutex::new(admission),
            degradation,
            accepting: AtomicBool::new(true),
        }
    }

    pub(super) fn integrity(&self) -> market_squawk_domain::CaptureIntegrityState {
        self.degradation.integrity()
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(super) enum GenerationPreparationError {
    #[error("generation retained graph failed: {0}")]
    Retained(CaptureRetainedSizeError),
    #[error("generation capture-memory reservation failed: {0}")]
    Accounting(CaptureAccountingError),
}

pub(super) fn mark_bundle_incomplete<B: CaptureAuthorityBundle>(bundle: B) {
    let (_initializer, _admission, degradation) = bundle.into_parts();
    degradation.mark_incomplete();
}

pub(super) fn try_prepare_generation<B: CaptureAuthorityBundle>(
    bundle: B,
    accounting: &Arc<CaptureMemoryAccounting>,
) -> Result<(B::Initializer, Arc<GenerationCaptureState<B>>), GenerationPreparationError> {
    let bundle_retained_bytes = match bundle.checked_retained_bytes() {
        Ok(bytes) => bytes,
        Err(error) => {
            mark_bundle_incomplete(bundle);
            return Err(GenerationPreparationError::Retained(error));
        }
    };
    let identity = bundle.identity();
    let identity_dynamic_bytes = match identity.checked_dynamic_retained_bytes() {
        Ok(bytes) => bytes,
        Err(error) => {
            mark_bundle_incomplete(bundle);
            return Err(GenerationPreparationError::Retained(error));
        }
    };
    let accounted_identity_bytes = match checked_arc_value_allocation_bytes::<
        AccountedGenerationIdentity,
    >(identity_dynamic_bytes)
    {
        Ok(bytes) => bytes,
        Err(_error) => {
            mark_bundle_incomplete(bundle);
            return Err(GenerationPreparationError::Retained(
                CaptureRetainedSizeError::Overflow {
                    component: CaptureRetainedComponent::PlatformIdentity,
                },
            ));
        }
    };
    let generation_state_bytes =
        match checked_arc_value_allocation_bytes::<GenerationCaptureState<B>>(0) {
            Ok(bytes) => bytes,
            Err(_error) => {
                mark_bundle_incomplete(bundle);
                return Err(GenerationPreparationError::Retained(
                    CaptureRetainedSizeError::Overflow {
                        component: CaptureRetainedComponent::PlatformGeneration,
                    },
                ));
            }
        };
    let complete_retained_bytes = match bundle_retained_bytes
        .checked_add(generation_state_bytes)
        .and_then(|bytes| bytes.checked_add(accounted_identity_bytes))
    {
        Some(bytes) => bytes,
        None => {
            mark_bundle_incomplete(bundle);
            return Err(GenerationPreparationError::Retained(
                CaptureRetainedSizeError::Overflow {
                    component: CaptureRetainedComponent::PlatformGeneration,
                },
            ));
        }
    };
    let resident = match accounting.try_reserve(
        AccountingComponent::ResidentGeneration,
        complete_retained_bytes,
    ) {
        Ok(resident) => resident,
        Err(error) => {
            mark_bundle_incomplete(bundle);
            return Err(GenerationPreparationError::Accounting(error));
        }
    };
    let (initializer, admission, degradation) = bundle.into_parts();
    let identity = Arc::new(AccountedGenerationIdentity {
        identity,
        complete_retained_bytes,
        resident,
    });
    Ok((
        initializer,
        Arc::new(GenerationCaptureState::new(
            identity,
            admission,
            degradation,
        )),
    ))
}
