//! Single-owner capture-allocation lifecycle control.

use std::sync::{Arc, atomic::Ordering};

use market_squawk_domain::{
    CaptureAuthorityBundle, CaptureAuthorityError, CaptureAuthorityIdentity, CaptureDegradation,
    CaptureInitializer, CaptureIntegrityState,
};
use thiserror::Error;

use super::{
    CaptureHealthReason, CaptureHealthSnapshot, CaptureIdentitySnapshot, CaptureState,
    GenerationPreparationError, WRITER_RUNNING, try_prepare_generation,
};

/// Whole-bundle generation transition failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CaptureGenerationError {
    /// Source, metadata revision, or session changed within one channel.
    #[error("capture generation rotation changed the registered source/session binding")]
    BindingMismatch {
        /// Active exact audit identity.
        current: CaptureHealthSnapshot,
        /// Received exact audit identity.
        received: CaptureHealthSnapshot,
    },
    /// Recovery requires a strictly newer registry-issued generation.
    #[error("capture generation must strictly advance")]
    NotNewer,
    /// A dead writer cannot be made healthy by changing authority bundles.
    #[error("capture writer is unavailable; create and synchronize a new capture channel")]
    WriterUnavailable,
    /// Concrete registry initialization failed closed.
    #[error("capture generation initialization failed: {0}")]
    Authority(#[from] CaptureAuthorityError),
    /// Successor retained-size preparation failed before predecessor revocation.
    #[error("capture generation preparation failed: {0}")]
    Preparation(market_squawk_domain::CaptureRetainedSizeError),
    /// The unified per-channel capture-memory ceiling rejected the complete successor.
    #[error("capture memory requires {required} bytes but ceiling is {ceiling} bytes")]
    CaptureMemoryBudgetExceeded {
        /// Total bytes required with the successor resident.
        required: usize,
        /// Configured channel ceiling.
        ceiling: usize,
    },
    /// Unified accounting entered a terminal fail-closed state.
    #[error("capture accounting invariant failed")]
    AccountingInvariant,
}

/// Non-clone owner of positive capture-allocation lifecycle transitions.
#[derive(Debug)]
pub struct RawCaptureControl<B: CaptureAuthorityBundle> {
    pub(super) state: Arc<CaptureState<B>>,
    pub(super) initializer: Option<B::Initializer>,
}

impl<B: CaptureAuthorityBundle> RawCaptureControl<B> {
    /// Irreversibly invalidates the current allocation without creating a successor.
    pub fn invalidate_current(&mut self) {
        let active = self.state.active.load_full();
        self.state
            .mark_incomplete_for_generation(&active, CaptureHealthReason::SupervisorStopped);
    }

    /// Activates the initial registry allocation after its supervised writer is running.
    pub fn activate_initial(&mut self) -> Result<(), CaptureGenerationError> {
        let _transition = match self.state.lifecycle_transition.lock() {
            Ok(transition) => transition,
            Err(poisoned) => poisoned.into_inner(),
        };
        let active = self.state.active.load_full();
        if self.state.writer_lifecycle.load(Ordering::Acquire) != WRITER_RUNNING {
            return Err(CaptureGenerationError::WriterUnavailable);
        }
        let Some(initializer) = self.initializer.as_mut() else {
            return if active.integrity() == CaptureIntegrityState::Healthy {
                Ok(())
            } else {
                Err(CaptureGenerationError::Authority(
                    CaptureAuthorityError::GenerationIncomplete,
                ))
            };
        };
        if let Err(error) = initializer.mark_healthy() {
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::AuthorityRejected);
            return Err(error.into());
        }
        self.initializer = None;
        Ok(())
    }

    /// Replaces the active allocation using a new whole registry-issued authority bundle.
    ///
    /// The complete successor is validated and initialized before the old allocation is
    /// Release-invalidated and RCU-replaced. A rejected successor is degraded without disturbing
    /// a still-current healthy predecessor.
    pub fn rotate_generation(&mut self, bundle: B) -> Result<(), CaptureGenerationError> {
        let (mut initializer, prepared) = try_prepare_generation(bundle, &self.state.accounting)
            .map_err(|error| match error {
                GenerationPreparationError::Retained(error) => {
                    CaptureGenerationError::Preparation(error)
                }
                GenerationPreparationError::Accounting(
                    super::accounting::CaptureAccountingError::BudgetExceeded { required, ceiling },
                ) => CaptureGenerationError::CaptureMemoryBudgetExceeded { required, ceiling },
                GenerationPreparationError::Accounting(
                    super::accounting::CaptureAccountingError::ArithmeticOverflow
                    | super::accounting::CaptureAccountingError::TransitionOverflow
                    | super::accounting::CaptureAccountingError::EpochOverflow
                    | super::accounting::CaptureAccountingError::InvariantViolated,
                ) => CaptureGenerationError::AccountingInvariant,
            })?;
        let _transition = match self.state.lifecycle_transition.lock() {
            Ok(transition) => transition,
            Err(poisoned) => poisoned.into_inner(),
        };
        let active = self.state.active.load_full();
        if !same_session(&active.identity.identity, &prepared.identity.identity) {
            prepared.degradation.mark_incomplete();
            return Err(CaptureGenerationError::BindingMismatch {
                current: CaptureHealthSnapshot {
                    identity: CaptureIdentitySnapshot(Arc::clone(&active.identity)),
                    integrity: active.integrity(),
                },
                received: CaptureHealthSnapshot {
                    identity: CaptureIdentitySnapshot(Arc::clone(&prepared.identity)),
                    integrity: prepared.integrity(),
                },
            });
        }
        if prepared.identity.identity.connection_generation()
            <= active.identity.identity.connection_generation()
        {
            prepared.degradation.mark_incomplete();
            return Err(CaptureGenerationError::NotNewer);
        }
        if self.state.writer_lifecycle.load(Ordering::Acquire) != WRITER_RUNNING {
            prepared.degradation.mark_incomplete();
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::WriterUnavailable);
            return Err(CaptureGenerationError::WriterUnavailable);
        }
        if let Err(error) = initializer.mark_healthy() {
            prepared.degradation.mark_incomplete();
            return Err(error.into());
        }
        if self.state.writer_lifecycle.load(Ordering::Acquire) != WRITER_RUNNING {
            prepared.degradation.mark_incomplete();
            self.state
                .mark_incomplete_for_generation(&active, CaptureHealthReason::WriterUnavailable);
            return Err(CaptureGenerationError::WriterUnavailable);
        }
        self.state
            .mark_incomplete_for_generation(&active, CaptureHealthReason::SupervisorStopped);
        self.state.active.store(prepared);
        if self.state.writer_lifecycle.load(Ordering::Acquire) != WRITER_RUNNING {
            let installed = self.state.active.load_full();
            self.state
                .mark_incomplete_for_generation(&installed, CaptureHealthReason::WriterUnavailable);
            return Err(CaptureGenerationError::WriterUnavailable);
        }
        self.initializer = None;
        Ok(())
    }

    /// Returns active audit identity.
    pub fn identity(&self) -> CaptureHealthSnapshot {
        let active = self.state.active.load_full();
        CaptureHealthSnapshot {
            identity: CaptureIdentitySnapshot(Arc::clone(&active.identity)),
            integrity: active.integrity(),
        }
    }
}

fn same_session(current: &CaptureAuthorityIdentity, received: &CaptureAuthorityIdentity) -> bool {
    current.source_id() == received.source_id()
        && current.metadata_revision() == received.metadata_revision()
        && current.session_identifier() == received.session_identifier()
}

impl<B: CaptureAuthorityBundle> Drop for RawCaptureControl<B> {
    fn drop(&mut self) {
        self.invalidate_current();
    }
}
